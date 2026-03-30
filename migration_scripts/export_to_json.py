#!/usr/bin/env python3
"""
export_to_json.py
=================
Export the SQLite database back to the original JSON format for rollback
or use with the old FastAPI backend.

Usage:
    python export_to_json.py --db /config/filament_stock.db --out-dir ./exported/
"""

import argparse, json, os, sqlite3, sys
from datetime import datetime
from pathlib import Path


def conn(db):
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c


def export_filaments(c, out):
    rows = c.execute("""
        SELECT sku, brand, colour, material, name, full_spools, partial_spools,
               spool_cost, spool_weight, image_url, url FROM filaments
        WHERE sku != 'misc' ORDER BY brand, colour
    """).fetchall()
    data = [{
        "sku": r["sku"], "brand": r["brand"], "colour": r["colour"],
        "material": r["material"], "name": r["name"],
        "full_spools": r["full_spools"], "partial_spools": r["partial_spools"],
        "spool_cost": r["spool_cost"], "spool_weight": r["spool_weight"],
        "image": r["image_url"], "url": r["url"],
    } for r in rows]
    (out / "filaments.json").write_text(json.dumps(data, indent=2, ensure_ascii=False), "utf-8")
    print(f"✓ filaments.json           {len(data):>4} records")


def export_live_prices(c, out):
    rows = c.execute(
        "SELECT sku, spool_cost, on_sale, price_live, error FROM filament_live_prices"
    ).fetchall()
    state = c.execute("SELECT value FROM app_state WHERE key='last_price_update'").fetchone()
    data = {r["sku"]: {"spool_cost": r["spool_cost"], "on_sale": bool(r["on_sale"]),
                        "live": bool(r["price_live"]), "error": r["error"]} for r in rows}
    data["last_update"] = state["value"] if state else datetime.now().isoformat()
    (out / "live_filament_info.json").write_text(json.dumps(data, indent=2, ensure_ascii=False), "utf-8")
    print(f"✓ live_filament_info.json  {len(rows):>4} records")


def export_cart(c, out):
    rows = c.execute(
        "SELECT sku, listed_qty, ordered_qty FROM filament_cart WHERE listed_qty>0 OR ordered_qty>0"
    ).fetchall()
    data = [{"id": r["sku"], "listed_qty": r["listed_qty"], "ordered_qty": r["ordered_qty"]}
            for r in rows]
    (out / "cart.json").write_text(json.dumps(data, indent=2, ensure_ascii=False), "utf-8")
    print(f"✓ cart.json                {len(data):>4} records")


def export_print_info(c, out):
    products = c.execute("SELECT * FROM print_info_products ORDER BY name").fetchall()
    data = {}
    for p in products:
        variants_rows = c.execute(
            "SELECT * FROM print_info_variants WHERE product_name=?", (p["name"],)).fetchall()
        variants = []
        for v in variants_rows:
            filaments = c.execute(
                "SELECT filament_id, grams FROM print_info_filaments WHERE variant_id=?",
                (v["id"],)).fetchall()
            variants.append({
                "variant_name": v["variant_name"], "sku": v["sku"],
                "filaments": [{"id": f["filament_id"], "grams": f["grams"]} for f in filaments],
            })
        data[p["name"]] = {
            "name": p["name"],
            "print_hours": p["print_hours"],
            "print_minutes": p["print_minutes"],
            "variants": variants,
        }
    (out / "print_info.json").write_text(json.dumps(data, indent=2, ensure_ascii=False), "utf-8")
    print(f"✓ print_info.json          {len(data):>4} products")


def main():
    p = argparse.ArgumentParser(description="Export SQLite → JSON for rollback.")
    p.add_argument("--db",      required=True, metavar="PATH")
    p.add_argument("--out-dir", required=True, metavar="PATH")
    args = p.parse_args()

    if not os.path.isfile(args.db):
        print(f"✗ DB not found: {args.db}", file=sys.stderr); sys.exit(1)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"\n📦  SQLite → JSON Export  ({args.db})\n")
    c = conn(args.db)
    export_filaments(c, out)
    export_live_prices(c, out)
    export_cart(c, out)
    export_print_info(c, out)
    c.close()
    print(f"\n🎉  Export complete → {out}\n")


if __name__ == "__main__":
    main()
