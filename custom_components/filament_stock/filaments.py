"""
filaments.py — Filament management business logic.
"""
import math
import logging
from typing import Optional
from .database import (
    get_all_filaments, get_filament, upsert_filament, update_filament_counts,
    delete_filament_row, db_add_to_cart, db_mark_ordered, db_mark_arrived,
    get_cart as _db_get_cart, log_print_job, get_last_price_update,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_FILAMENT_COST_PER_1KG_ROLL = 30
DEFAULT_FILAMENT_COST_PER_500G_ROLL = 15
DEFAULT_FILAMENT_ROLL_WEIGHT = 1000

DEAL_DICT = {
    0: "",
    1: "I'm cheaper because I'm not quite right",
    2: "or 2 for $10.00",
    3: "or 3 for $10.00",
    4: "Add an egg for $17 more",
    5: "Add an egg for $19 more",
    6: "Add mini dragon for $5 more",
    7: "2 for $10 or 3 for $15",
    8: "End of Production",
    9: "I glow in the dark",
    10: "I change colour with heat",
    11: "or add 3x minis for $5.00 more",
}

STICKER_KEY_NAMES = {1: "Tag", 2: "Sticker (4-up)", 3: "Egg", 4: "Sticker No Price (4-up)"}


def _build_sku(brand, material, colour):
    return f"{brand}-{material}-{colour}".lower().replace(" ", "_")


def _effective_price(row: dict):
    live_cost   = row.get("live_cost")
    stored_cost = row.get("spool_cost")
    price_live  = bool(row.get("price_live", 0))
    on_sale     = bool(row.get("on_sale", 0))

    if price_live and live_cost is not None:
        return live_cost, False, True, on_sale
    if live_cost is not None:
        return live_cost, False, False, on_sale
    if stored_cost is not None:
        return stored_cost, False, False, False

    spool_weight = row.get("spool_weight") or DEFAULT_FILAMENT_ROLL_WEIGHT
    ratio        = spool_weight / DEFAULT_FILAMENT_ROLL_WEIGHT
    return DEFAULT_FILAMENT_COST_PER_1KG_ROLL * ratio, True, False, False


def build_filament_response(row: dict) -> dict:
    price, is_default, is_live, on_sale = _effective_price(row)
    warnings = []
    live_error = row.get("live_error")
    if not is_live and live_error:
        warnings.append(f"Error scraping price for {row.get('sku')}: {live_error}")
    if is_default:
        warnings.append(f"No price found for {row.get('sku')}. Using default.")
    return {
        "brand":       row.get("brand"),
        "colour":      row.get("colour"),
        "material":    row.get("material"),
        "spool_cost":  price,
        "spool_weight": row.get("spool_weight"),
        "image_url":   row.get("image_url"),
        "filament_id": row.get("sku"),
        "url":         row.get("url"),
        "counts": {"full": row.get("full_spools", 0), "partial": row.get("partial_spools", 0)},
        "status_info": {
            "is_default_price": is_default,
            "price_is_live":    is_live,
            "on_sale":          on_sale,
            "error":            live_error,
            "warnings":         warnings,
        },
        "cart_status": {
            "listed":  row.get("listed_qty") or 0,
            "ordered": row.get("ordered_qty") or 0,
        },
    }


def get_all(db_path):
    try:
        return [build_filament_response(r) for r in get_all_filaments(db_path)], None
    except Exception as e:
        return None, str(e)


def get_info(db_path):
    last = get_last_price_update(db_path)
    return {
        "default_filament_cost_per_1kg_roll":  DEFAULT_FILAMENT_COST_PER_1KG_ROLL,
        "default_filament_cost_per_500g_roll": DEFAULT_FILAMENT_COST_PER_500G_ROLL,
        "default_filament_roll_weight":        DEFAULT_FILAMENT_ROLL_WEIGHT,
        "last_price_update": last.isoformat() if last else None,
    }


def new_spool(db_path, filament_id=None, full=0, partial=0,
              brand=None, colour=None, material=None,
              spool_cost=None, spool_weight=None, image_url=None, url=None):
    try:
        if filament_id is None:
            if not all([brand, material, colour]):
                return False, "brand, material, and colour required for new filament."
            sku = _build_sku(brand, material, colour)
            upsert_filament(db_path, sku, brand=brand, colour=colour, material=material,
                            name=f"{brand} {material} {colour}", full_spools=full,
                            partial_spools=partial, spool_cost=spool_cost,
                            spool_weight=spool_weight, image_url=image_url, url=url)
            return True, None
        return update_filament_counts(db_path, filament_id, full_delta=full, partial_delta=partial)
    except Exception as e:
        return False, str(e)


def open_spool(db_path, filament_id):
    row = get_filament(db_path, filament_id)
    if not row: return False, "Filament not found."
    if (row.get("full_spools") or 0) <= 0: return False, "No full spools available."
    return update_filament_counts(db_path, filament_id, full_delta=-1, partial_delta=1)


def finish_spool(db_path, filament_id):
    row = get_filament(db_path, filament_id)
    if not row: return False, "Filament not found."
    if (row.get("partial_spools") or 0) <= 0: return False, "No partial spools available."
    return update_filament_counts(db_path, filament_id, partial_delta=-1)


def set_spool(db_path, filament_id=None, full=0, partial=0, **kwargs):
    if filament_id is None:
        return new_spool(db_path, filament_id=None, full=full, partial=partial, **kwargs)
    return update_filament_counts(db_path, filament_id, full_set=full, partial_set=partial)


def modify_spool(db_path, filament_id, brand=None, colour=None, material=None,
                 spool_cost=None, spool_weight=None, image_url=None, url=None):
    row = get_filament(db_path, filament_id)
    if not row: return False, "Filament not found."
    updates = {}
    for k, v in [("brand", brand), ("colour", colour), ("material", material),
                 ("spool_cost", spool_cost), ("spool_weight", spool_weight),
                 ("image_url", image_url), ("url", url)]:
        if v is not None: updates[k] = v
    if any(k in updates for k in ("brand", "colour", "material")):
        b = updates.get("brand", row.get("brand", ""))
        m = updates.get("material", row.get("material", ""))
        c = updates.get("colour", row.get("colour", ""))
        updates["name"] = f"{b} {m} {c}".strip()
    try:
        upsert_filament(db_path, filament_id, **updates)
        return True, None
    except Exception as e:
        return False, str(e)


def delete_spool(db_path, filament_id):
    return delete_filament_row(db_path, filament_id)


def add_to_cart(db_path, filament_id, qty):
    if qty <= 0: return False, "Quantity must be positive."
    return db_add_to_cart(db_path, filament_id, qty)


def mark_ordered(db_path, filament_id, qty):
    if qty <= 0: return False, "Quantity must be positive."
    return db_mark_ordered(db_path, filament_id, qty)


def mark_arrived(db_path, filament_id, qty):
    if qty <= 0: return False, "Quantity must be positive."
    return db_mark_arrived(db_path, filament_id, qty)


def get_cart(db_path):
    return _db_get_cart(db_path)


def resolve_print_params(qty, sku, price, item_name, variant_name,
                          sticker_key, deal_key, template_paths):
    try:
        dk = int(deal_key)
    except (TypeError, ValueError):
        dk = None

    if dk is not None and dk in DEAL_DICT:
        deal_text = DEAL_DICT[dk]
    elif isinstance(deal_key, str) and deal_key:
        deal_text = deal_key
    else:
        return None, {"message": "Invalid Deal key", "options": DEAL_DICT}

    try:
        sk = int(sticker_key)
    except (TypeError, ValueError):
        return None, {"message": "Invalid Sticker key", "options": STICKER_KEY_NAMES}

    tmap = {1: template_paths.get("tag"), 2: template_paths.get("sticker"),
            3: template_paths.get("egg"), 4: template_paths.get("small_sticker")}
    if sk not in tmap or not tmap[sk]:
        return None, {"message": "Invalid Sticker key", "options": STICKER_KEY_NAMES}

    price_str = f"${price:.2f}"
    if sk == 1:
        fields = {"Title": item_name, "Variant Barcode": sku, "Price": price_str,
                  "Colour": variant_name, "Deal Tag": deal_text}
        pqty = qty
    elif sk == 2:
        fields = {f"Variant Barcode {i}": sku for i in range(1, 5)}
        fields.update({f"Tag $ {i}": price_str for i in range(1, 5)})
        pqty = math.ceil(qty / 4)
    elif sk == 3:
        fields = {"Title": item_name, "Deal Tag": deal_text, "Colour": variant_name,
                  "Price": price_str, "Variant Barcode": sku}
        pqty = qty
    elif sk == 4:
        fields = {f"Variant Barcode {i}": sku for i in range(1, 5)}
        pqty = math.ceil(qty / 4)
    else:
        return None, {"message": "Unknown sticker key"}

    return {"template": tmap[sk], "fields": fields, "quantity": pqty}, None
