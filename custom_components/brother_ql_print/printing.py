#!/usr/bin/env python3
"""
lbx_to_png.py — Render Brother P-touch .lbx label files to PNG.

Usage:
    python lbx_to_png.py label.lbx output.png
    python lbx_to_png.py label.lbx output.png --merge "Title=My Product" "Colour=Blue" "Price=$29.99"
    python lbx_to_png.py label.lbx output.png --dpi 300

.lbx files are ZIP archives containing a 'label.xml' file.

-----------------------------------------------------------------------
COORDINATE SYSTEM
-----------------------------------------------------------------------
LBX XML axes (confirmed by working QR code):
  XML x  = position ACROSS the tape  (short axis, 0..paper_height)
  XML y  = position ALONG  the tape  (long  axis, 0..paper_width)
  XML w  = extent   ACROSS the tape
  XML h  = extent   ALONG  the tape

PIL canvas is created as (tape_len_px, tape_wide_px):
  PIL columns (left→right, axis 0) = y direction (along tape)
  PIL rows    (top→bottom, axis 1) = x direction (across tape)

Every object's on-canvas footprint is therefore:
  cols  y_px .. y_px + h_px   (along tape)
  rows  x_px .. x_px + w_px   (across tape)

Sub-image size (pre drawn, before content rotation):
  sub PIL width  = h_px   (along tape  = number of columns it occupies)
  sub PIL height = w_px   (across tape = number of rows it occupies)

Content rotation (the `angle` attribute):
  The angle rotates the CONTENT within the fixed h_px × w_px footprint.
  We rotate with expand=False so the footprint dimensions never change.
  PIL.rotate is CCW; LBX angle is CW → PIL angle = -angle (or 360-angle).

  angle=  0° → no rotation
  angle= 90° → PIL rotate(-90) or rotate(270)
  angle=180° → PIL rotate(180)
  angle=270° → PIL rotate(-270) or rotate(90)  ← most common for portrait text

After all objects are drawn, rotate the canvas -90° (90°CW) so the tape
reads left→right in the final portrait PNG output.
"""

import argparse
import sys
import zipfile
import xml.etree.ElementTree as ET
import asyncio
from pathlib import Path
from brother_ql.labels import ALL_LABELS, FormFactor
from brother_ql.raster import BrotherQLRaster
from brother_ql.conversion import convert
from brother_ql.backends.helpers import discover, send
from brother_ql.backends.pyusb import BrotherQLBackendPyUSB as pyusb_backend
try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    sys.exit("Pillow is required:  pip install Pillow")

NS = {
    "pt":      "http://schemas.brother.info/ptouch/2007/lbx/main",
    "style":   "http://schemas.brother.info/ptouch/2007/lbx/style",
    "text":    "http://schemas.brother.info/ptouch/2007/lbx/text",
    "barcode": "http://schemas.brother.info/ptouch/2007/lbx/barcode",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def pt_to_px(pt_str: str, dpi: float) -> float:
    return float(str(pt_str).replace("pt", "").strip()) / 72.0 * dpi

def hex_to_rgb(h: str):
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

def wrap_text(text, font, max_width):
    words = text.split()
    lines = []
    current = ""

    for word in words:
        test = current + (" " if current else "") + word
        bbox = font.getbbox(test)
        width = bbox[2] - bbox[0]
        if width <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines

def find_fontsize(
    text: str,
    font_path: str,
    target_width_px: int,
    min_size: int = 1,
    max_size: int = 500,
) -> int:
    print(f"Finding font size for '{text}' to fit within {target_width_px}px...")
    img = Image.new("L", (1, 1))
    draw = ImageDraw.Draw(img)

    def text_width(size: int) -> int:
        font = ImageFont.truetype(font_path, size)
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0]

    best = min_size

    while min_size <= max_size:
        mid = (min_size + max_size) // 2
        width = text_width(mid)

        if width <= target_width_px:
            best = mid
            min_size = mid + 1
        else:
            max_size = mid - 1

    print(f"Best font size: {best}px (text width: {text_width(best)}px)")
    
    return best

def fit_text_block(text, font_name, bold, italic, max_w, max_h, auto_lf):
    size = 200  # start large (faster than growing upward)

    while size > 6:
        font, _ = load_font(font_name, size, bold, italic)

        if auto_lf:
            lines = wrap_text(text, font, max_w)
        else:
            lines = [text]

        line_height = font.getbbox("Ag")[3]
        total_height = line_height * len(lines)

        too_wide = any(
            (font.getbbox(line)[2] - font.getbbox(line)[0]) > max_w
            for line in lines
        )

        too_tall = total_height > max_h

        if not too_wide and not too_tall:
            return font, lines

        size -= 1  # step down

    return font, lines  # fallback (tiny)

def place_object(canvas: Image.Image,
                 sub: Image.Image,
                 x_px: float, y_px: float,
                 angle: int):
    """
    Paste sub onto canvas.
    sub must already be sized h_px wide × w_px tall (along × across).
    Content rotation by `angle` degrees CW has already been applied to sub.
    Paste position: col = y_px (along), row = x_px (across).
    """
    new_x = canvas.height - (x_px + sub.height)
    new_y = y_px

    canvas.paste(sub, (int(round(new_y)), int(round(new_x))), sub)

# ---------------------------------------------------------------------------
# Font loading
# ---------------------------------------------------------------------------

def load_font(name: str, size_px: float, bold: bool = False, italic: bool = False):
    size_px = max(8, int(round(size_px)))
    candidates = []
    for stem in [name, name.replace(" ", ""), name.lower(), name.lower().replace(" ", "")]:
        if bold and italic:
            candidates += [f"{stem}bi.ttf", f"{stem}-BoldItalic.ttf"]
        if bold:
            candidates += [f"{stem}bd.ttf", f"{stem}b.ttf",
                           f"{stem}-Bold.ttf", f"{stem}-bold.ttf"]
        if italic:
            candidates += [f"{stem}i.ttf", f"{stem}-Italic.ttf"]
        candidates += [f"{stem}.ttf", f"{stem}-Regular.ttf"]

    search_dirs = [
        r"C:\Windows\Fonts",
        "/usr/share/fonts/truetype/liberation",
        "/usr/share/fonts/truetype/dejavu",
        "/usr/share/fonts/truetype/freefont",
        "/usr/share/fonts/truetype/msttcorefonts",
        "/usr/share/fonts/truetype",
        "/usr/share/fonts/opentype",
        "/System/Library/Fonts",
        "/Library/Fonts",
        str(Path.home() / "Library" / "Fonts"),
        ".",
    ]
    fallbacks = [
        "LiberationSans-Bold.ttf", "LiberationSans-Regular.ttf",
        "DejaVuSans-Bold.ttf",     "DejaVuSans.ttf",
        "FreeSansBold.ttf",        "FreeSans.ttf",
    ]
    for directory in search_dirs:
        p = Path(directory)
        if not p.exists():
            continue
        for cand in candidates + fallbacks:
            fp = p / cand
            if fp.exists():
                try:
                    return ImageFont.truetype(str(fp), size_px), str(fp)
                except Exception:
                    pass
            for fp2 in p.rglob(cand):
                try:
                    return ImageFont.truetype(str(fp2), size_px), str(fp2)
                except Exception:
                    pass
    return ImageFont.load_default(), None

# ---------------------------------------------------------------------------
# Text rendering
# ---------------------------------------------------------------------------

def render_text_object(canvas: Image.Image, obj_el: ET.Element, dpi: float, merge: dict):
    style_el = obj_el.find("pt:objectStyle", NS)
    if style_el is None:
        return

    x_px  = pt_to_px(style_el.get("x",      "0pt"), dpi)   # across tape
    y_px  = pt_to_px(style_el.get("y",      "0pt"), dpi)   # along tape
    w_px  = pt_to_px(style_el.get("width",  "50pt"), dpi)  # across tape extent
    h_px  = pt_to_px(style_el.get("height", "20pt"), dpi)  # along tape extent
    angle = int(style_el.get("angle", "0"))
    back_color = (0, 0, 0, 0)

    exp_el   = style_el.find("pt:expanded", NS)
    obj_name = exp_el.get("objectName", "") if exp_el is not None else ""

    data_el  = obj_el.find("pt:data", NS)
    raw_text = (data_el.text or "") if data_el is not None else ""

    for key, val in merge.items():
        if key.lower() == obj_name.lower():
            raw_text = val
            break

    # Font
    font_info  = obj_el.find("text:ptFontInfo", NS)
    font_name  = "Arial"
    font_size  = 10.0
    bold       = False
    italic     = False
    text_color = (0, 0, 0)
    if font_info is not None:
        lf = font_info.find("text:logFont", NS)
        fe = font_info.find("text:fontExt", NS)
        if lf is not None:
            font_name = lf.get("name", "Arial")
            bold      = int(lf.get("weight", "400")) >= 700
            italic    = lf.get("italic", "false").lower() == "true"
        if fe is not None:
            font_size  = float(fe.get("size", "10pt").replace("pt", ""))
            text_color = hex_to_rgb(fe.get("textColor", "#000000"))

    font_size_px = pt_to_px(f"{font_size}pt", dpi)

    align_el = obj_el.find("text:textAlign", NS)
    h_align  = "LEFT"
    v_align  = "TOP"
    if align_el is not None:
        h_align = align_el.get("horizontalAlignment", "LEFT").upper()
        v_align = align_el.get("verticalAlignment",   "TOP").upper()

    ctrl_el = obj_el.find("text:textControl", NS)
    shrink  = ctrl_el is not None and ctrl_el.get("shrink", "false").lower() == "true"

    # Sub image: h_px wide (along tape) × w_px tall (across tape)
    # Text is drawn horizontally in this image — the long dimension gives plenty of room.
    if angle in (0, 180):
        sub_w = int(round(w_px))
        sub_h = int(round(h_px))
    else:  # 90 or 270
        sub_w = int(round(h_px))
        sub_h = int(round(w_px))
    
    sub   = Image.new("RGBA", (sub_w, sub_h), back_color)
    draw  = ImageDraw.Draw(sub)

    print(f"Rendering text object '{obj_name}' at ({x_px:.1f}px, {y_px:.1f}px) size ({w_px:.1f}px × {h_px:.1f}px) angle {angle}°")
    print(f"initial font: '{font_name}' {font_size}px {'bold' if bold else 'normal'} {'italic' if italic else ''}  align={h_align}/{v_align}  shrink={shrink}")
    print(f"Sub-image size: {sub_w} × {sub_h}")

    # Load font normally
    font, font_path = load_font(font_name, font_size_px, bold, italic)
    print(f"Loaded font: {font_path} at size {font_size_px}px")

    # JUSTIFY logic
    if h_align == "JUSTIFY" and font_path:
        # Determine the target width depending on rotation
        if angle in (90, 270):
            target_width = sub_h
        else:
            target_width = sub_w
        
        # Find the maximum font size that allows the text to fit the width
        font_size_px = find_fontsize(raw_text, font_path, target_width)
        font = ImageFont.truetype(font_path, font_size_px)

    bb     = font.getbbox(raw_text)
    text_w = bb[2] - bb[0]
    text_h = bb[3] - bb[1]

    if h_align in ("RIGHT", "JUSTIFY"):
        tx = sub_w - text_w - 2
    elif h_align == "CENTER":
        tx = (sub_w - text_w) // 2
    else:
        tx = 2

    if v_align == "CENTER":
        ty = (sub_h - text_h) // 2
    elif v_align == "BOTTOM":
        ty = sub_h - text_h - 2
    else:
        ty = 2

    wrap_width = sub_w

    auto_lf = ctrl_el is not None and ctrl_el.get("autoLF", "true").lower() == "true"

    if auto_lf:
        lines = wrap_text(raw_text, font, wrap_width)
    else:
        lines = [raw_text]

    if shrink:
        test_size = font_size_px

        while test_size > 6:
            font, font_path = load_font(font_name, test_size, bold, italic)

            if auto_lf:
                test_lines = wrap_text(raw_text, font, wrap_width)
            else:
                test_lines = [raw_text]

            line_height = font.getbbox("Ag")[3]
            total_height = line_height * len(test_lines)

            too_wide = any(
                (font.getbbox(l)[2] - font.getbbox(l)[0]) > wrap_width
                for l in test_lines
            )

            too_tall = total_height > sub_h

            if not too_wide and not too_tall:
                lines = test_lines
                break

            test_size -= 0.5

    line_height = font.getbbox("Ag")[3]
    total_height = line_height * len(lines)

    # vertical alignment
    if v_align == "CENTER":
        ty = (sub_h - total_height) // 2
    elif v_align == "BOTTOM":
        ty = sub_h - total_height - 2
    else:
        ty = 2

    for line in lines:
        bbox = font.getbbox(line)
        text_w = bbox[2] - bbox[0]

        if h_align in ("RIGHT", "JUSTIFY"):
            tx = sub_w - text_w - 2
        elif h_align == "CENTER":
            tx = (sub_w - text_w) // 2
        else:
            tx = 2
        
        draw.text((tx, ty), line, font=font, fill=text_color)

        ty += line_height

    # Rotate content within the fixed footprint (expand=False keeps h_px × w_px size)
    # PIL rotates CCW; LBX angle is CW → use negative angle
    sub = sub.rotate(-(angle - 90), expand=True, resample=Image.BICUBIC)

    place_object(canvas, sub, x_px, y_px, angle)


# ---------------------------------------------------------------------------
# Barcode / QR
# ---------------------------------------------------------------------------

def make_qr_image(data: str, size_px: int) -> Image.Image:
    try:
        import qrcode

        qr = qrcode.QRCode(
            error_correction=qrcode.constants.ERROR_CORRECT_Q,
            border=2,
            box_size=10
        )
        qr.add_data(data)
        qr.make(fit=True)

        # Generate black/white image first
        img = qr.make_image(fill_color="black", back_color="white").convert("RGBA")

        # Convert white → transparent
        datas = img.getdata()
        new_data = []

        for item in datas:
            # item is (R,G,B,A)
            if item[0] > 200 and item[1] > 200 and item[2] > 200:
                # white → transparent
                new_data.append((255, 255, 255, 0))
            else:
                # black stays solid
                new_data.append((0, 0, 0, 255))

        img.putdata(new_data)

        # ⚠️ IMPORTANT: use NEAREST for QR (no blur)
        img = img.resize((size_px, size_px), Image.NEAREST)

        return img

    except ImportError:
        img = Image.new("RGBA", (size_px, size_px), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        d.rectangle([0, 0, size_px-1, size_px-1], outline="black", width=2)
        d.line([0, 0, size_px, size_px], fill="black", width=2)
        d.line([0, size_px, size_px, 0], fill="black", width=2)
        d.text((4, size_px//2 - 8), "install qrcode", fill="black")
        return img


def render_barcode_object(canvas: Image.Image, obj_el: ET.Element, dpi: float, merge: dict):
    style_el = obj_el.find("pt:objectStyle", NS)
    if style_el is None:
        return

    x_px  = pt_to_px(style_el.get("x",      "0pt"), dpi)
    y_px  = pt_to_px(style_el.get("y",      "0pt"), dpi)
    w_px  = pt_to_px(style_el.get("width",  "40pt"), dpi)
    h_px  = pt_to_px(style_el.get("height", "40pt"), dpi)
    angle = int(style_el.get("angle", "0"))

    exp_el   = style_el.find("pt:expanded", NS)
    obj_name = exp_el.get("objectName", "") if exp_el is not None else ""

    data_el  = obj_el.find("pt:data", NS)
    raw_data = (data_el.text or "") if data_el is not None else ""

    for key, val in merge.items():
        if key.lower() == obj_name.lower():
            raw_data = val
            break

    bc_style = obj_el.find("barcode:barcodeStyle", NS)
    protocol = bc_style.get("protocol", "QRCODE").upper() if bc_style is not None else "QRCODE"

    # Sub: h_px wide × w_px tall (same footprint rule as text)
    if angle in (0, 180):
        sub_w = int(round(w_px))
        sub_h = int(round(h_px))
    else:  # 90 or 270
        sub_w = int(round(h_px))
        sub_h = int(round(w_px))

    if protocol == "QRCODE":
        size_px = min(sub_w, sub_h)
        qr = make_qr_image(raw_data, size_px)
        sub = Image.new("RGBA", (sub_w, sub_h), (0, 0, 0, 0))
        d   = ImageDraw.Draw(sub)
        # Centre QR in sub
        ox = (sub_w - qr.width)  // 2
        oy = (sub_h - qr.height) // 2
        sub.paste(qr, (ox, oy), qr)
        d.rectangle([0, 0, sub_w-1, sub_h-1], outline="red")  # debug: show sub image border
    else:
        sub = Image.new("RGB", (sub_w, sub_h), "white")
        d   = ImageDraw.Draw(sub)
        n   = max(10, len(raw_data) * 2)
        for i in range(n):
            if i % 3 != 1:
                bx = int(i * sub_w / n)
                d.rectangle([bx, 0, bx + max(1, int(sub_w/n)-1), int(sub_h*0.8)], fill="black")
        d.text((2, int(sub_h*0.82)), raw_data[:24], fill="black")

    if angle not in (90, 270):
        sub = sub.rotate(-(angle - 90), expand=False)

    place_object(canvas, sub, x_px, y_px, angle)


# ---------------------------------------------------------------------------
# Label renderer
# ---------------------------------------------------------------------------

def render_label(xml_str: str, merge: dict, dpi: float) -> Image.Image:
    root  = ET.fromstring(xml_str)
    paper = root.find(".//style:paper", NS)

    if paper is not None:
        tape_len_px  = pt_to_px(paper.get("width",  "175.7pt"), dpi)  # along tape
        tape_wide_px = pt_to_px(paper.get("height", "61pt"),    dpi)  # across tape
        paper_color  = hex_to_rgb(paper.get("paperColor", "#FFFFFF"))
    else:
        tape_len_px  = pt_to_px("175.7pt", dpi)
        tape_wide_px = pt_to_px("61pt",    dpi)
        paper_color  = (255, 255, 255)

    # Canvas: PIL cols = along tape (y), PIL rows = across tape (x)
    canvas = Image.new("RGB",
                       (int(round(tape_len_px)), int(round(tape_wide_px))),
                       paper_color)

    objects_el = root.find(".//pt:objects", NS)
    if objects_el is not None:
        for child in objects_el:
            tag = child.tag.lower()
            if "}text" in tag:
                render_text_object(canvas, child, dpi, merge)
            elif "}barcode" in tag:
                render_barcode_object(canvas, child, dpi, merge)

    # Rotate 90°CW: tape reads left→right in final portrait image
    return canvas.rotate(-90, expand=True)


# ---------------------------------------------------------------------------
# LBX reader
# ---------------------------------------------------------------------------

def read_lbx(path: str) -> str:
    with zipfile.ZipFile(path, "r") as zf:
        names    = zf.namelist()
        xml_name = next(
            (n for n in names if n.lower() == "label.xml"),
            next((n for n in names if n.lower().endswith(".xml")), None),
        )
        if xml_name is None:
            raise ValueError(f"No XML found in {path}. Contents: {names}")
        return zf.read(xml_name).decode("utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_merge(pairs) -> dict:
    result = {}
    for pair in pairs:
        if "=" not in pair:
            print(f"Warning: skipping '{pair}' — expected Key=Value", file=sys.stderr)
            continue
        k, _, v = pair.partition("=")
        result[k.strip()] = v.strip()
    return result

def render_lbx_to_image(template_path: str, merge: dict, dpi: float = 300, xml: bool = False) -> Image.Image:
    if xml:
        xml_str = Path(template_path).read_text(encoding="utf-8")
    else:
        try:
            xml_str = read_lbx(template_path)
        except zipfile.BadZipFile:
            xml_str = Path(template_path).read_text(encoding="utf-8")

    img = render_label(xml_str, merge, dpi)
    return img

def get_printer_and_label(model_override: str = None):
    """
    Discover the first connected USB Brother QL printer.
    Returns (printer_dict, backend_instance, label_str).
    label_str is always "62" — tape detection is not possible via brother_ql.
    """
    devices = discover('pyusb')
    if not devices:
        raise RuntimeError(
            "No Brother QL printer found via USB. "
            "Ensure the printer is connected and powered on."
        )

    printer = devices[0]

    # Apply model override from HA config if provided
    if model_override:
        printer = dict(printer)
        printer["model"] = model_override

    backend = pyusb_backend(printer["identifier"])
    label = "62"  # tape size must be configured separately

    return printer, backend, label

def prepare_image(img, label):
    """
    Prepare a Brother label image for printing.
    Assumes the image already respects the printable area/margins.
    """
    lbl_obj = next((l for l in ALL_LABELS if l.identifier == label), None)
    if lbl_obj is None:
        raise RuntimeError(f"Unsupported label: {label}")

    img = img.convert("L")

    w, h = img.size

    # Optional: clamp height (2x width rule)
    max_h = w * 2
    if h > max_h:
        img = img.crop((0, 0, w, max_h))

    # Convert to 1-bit
    img = img.point(lambda x: 0 if x < 128 else 255, '1')

    return img

from brother_ql.raster import BrotherQLRaster

def print_image(img, label="62", quantity=1, preview=False, printer_model: str = None, backend_identifier="pyusb"):
    """
    Send a PIL image to the Brother QL printer using the library's built-in print function.
    Handles backend, rasterization, and USB automatically.
    """

    if preview:
        img.show()
        return
    
    enqueue_print(img, label=label, quantity=quantity, model=printer_model, backend_identifier=backend_identifier)


PRINT_QUEUE = []

async def enqueue_print(img, label="62", quantity=1, model="QL-720NW", backend_identifier="pyusb"):
    """Add a print job to the queue."""
    PRINT_QUEUE.append({
        "img": img,
        "label": label,
        "quantity": quantity,
        "model": model,
        "backend": backend_identifier,
    })
    await process_queue()

async def process_queue():
    """Attempt to process jobs in the queue."""
    while PRINT_QUEUE:
        job = PRINT_QUEUE[0]
        try:
            # Attempt to print
            qlr = BrotherQLRaster(job["model"])
            qlr.exception_on_warning = True
            instructions = convert(
                qlr=qlr,
                images=[job["img"]],
                label=job["label"],
                rotate="auto",
                threshold=70,
                dither=False,
                compress=True,
                red=False,
                dpi_600=False,
                hq=True,
                cut=True,
            )

            for i in range(job["quantity"]):
                send(
                    instructions=instructions,
                    printer_identifier=None,  # will pick first discovered
                    backend_identifier=job["backend"],
                    blocking=True
                )

            # Success: remove job from queue
            PRINT_QUEUE.pop(0)

        except Exception as e:
            # Printer might be asleep, wait and retry
            print(f"Printer busy/asleep, retrying in 30s: {e}")
            await asyncio.sleep(30)

def main():
    parser = argparse.ArgumentParser(
        description="Render a Brother .lbx label to PNG.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python lbx_to_png.py tag_label.lbx output.png
  python lbx_to_png.py tag_label.lbx output.png --merge "Title=My Product" "Price=$29.99"
  python lbx_to_png.py tag_label.lbx output.png --dpi 300
  python lbx_to_png.py label.xml output.png --xml
        """,
    )
    parser.add_argument("lbx",    help="Path to .lbx file (or .xml with --xml)")
    parser.add_argument("output", help="Output PNG path")
    parser.add_argument("--merge", "-m", nargs="+", default=[], metavar="KEY=VALUE",
                        help="Override named fields, e.g.  Title='Blue Dragon'  Price=$9.99")
    parser.add_argument("--dpi", "-d", type=float, default=180.0,
                        help="Render DPI (default 180; QL-700 native = 300)")
    parser.add_argument("--xml", "-x", action="store_true",
                        help="Input is raw XML, not a .lbx ZIP")
    args  = parser.parse_args()
    merge = parse_merge(args.merge)

    if args.xml:
        xml_str = Path(args.lbx).read_text(encoding="utf-8")
    else:
        try:
            xml_str = read_lbx(args.lbx)
        except zipfile.BadZipFile:
            xml_str = Path(args.lbx).read_text(encoding="utf-8")

    img = render_label(xml_str, merge, args.dpi)
    img.save(args.output, "PNG")
    print(f"Saved {args.output}  ({img.width}x{img.height} px @ {args.dpi} dpi)")


def render_and_print(template_path: str, fields: dict, quantity: int = 1,
                     preview: bool = False, label: str = "62",
                     model_override: str = None):
    """
    Full pipeline: LBX → render → scale → print.
    Called by the HA integration via async_add_executor_job.
    """
    img = render_lbx_to_image(template_path, fields)
    img = prepare_image(img, label)
    print_image(img, label=label, quantity=quantity, preview=preview,
                model_override=model_override)

if __name__ == "__main__":
    render_and_print(r"C:\Users\amacc\OneDrive\Documents\GitHub\printing-tools\shared_resources\tag_label.lbx", {"Title": "Test Product", "Price": "$9.99", "Variant Barcode": "https://aidens3dp.com", "Colour": "Blue", "Deal Tag": "Limited Time Offer"}, quantity=1, preview=True)