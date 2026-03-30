"""
cost_calculator.py — Ported from shared_scripts/cost_calculator.py.

Calculates COGS (electricity + filament cost) for any SKU using
data from the SQLite database instead of JSON files.
"""
from typing import Optional
from .database import get_print_info_for_sku, get_filament

ELECTRICITY_RATE_PER_KWH = 0.3129   # AUD
HOURLY_CONSUMPTION_KW     = 0.2
DEFAULT_FILAMENT_COST_1KG = 30.0
DEFAULT_FILAMENT_COST_500G = 15.0
DEFAULT_ROLL_WEIGHT        = 1000    # grams


def get_filament_cost_per_gram(db_path: str, filament_id: str) -> float:
    row = get_filament(db_path, filament_id)
    if row is None:
        # Unknown filament — use default 1kg cost
        return DEFAULT_FILAMENT_COST_1KG / DEFAULT_ROLL_WEIGHT

    spool_weight = row.get("spool_weight") or DEFAULT_ROLL_WEIGHT

    # Prefer live price, then stored cost, then default
    live_cost  = row.get("live_cost") if row.get("price_live") else None
    stored_cost = row.get("spool_cost")
    spool_cost = live_cost or stored_cost
    if spool_cost is None:
        spool_cost = (DEFAULT_FILAMENT_COST_1KG if spool_weight >= 1000
                      else DEFAULT_FILAMENT_COST_500G)

    return spool_cost / spool_weight


def calculate_electricity_cost(hours: int, minutes: int) -> float:
    total_hours = hours + minutes / 60
    return HOURLY_CONSUMPTION_KW * total_hours * ELECTRICITY_RATE_PER_KWH


def calculate_filament_cost(db_path: str, filaments: list) -> float:
    """filaments = [{"id": str, "grams": float}, ...]"""
    total = 0.0
    for f in filaments:
        cpg = get_filament_cost_per_gram(db_path, f["id"])
        total += f["grams"] * cpg
    return total


def calculate_total_print_cost(db_path: str, hours: int, minutes: int, filaments: list) -> float:
    return calculate_electricity_cost(hours, minutes) + calculate_filament_cost(db_path, filaments)


def get_cogs(db_path: str, sku: str) -> Optional[float]:
    """Returns COGS for a SKU, or None if the SKU has no print info."""
    info = get_print_info_for_sku(db_path, sku)
    if not info:
        return None
    cost = calculate_total_print_cost(
        db_path,
        info["print_hours"],
        info["print_minutes"],
        info["filaments"],
    )
    return round(cost, 2)
