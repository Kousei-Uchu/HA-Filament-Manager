"""
sensor.py — Home Assistant entity layer for Filament Stock Manager.

Creates:
  • One DEVICE per filament, with sub-entities for every attribute
  • Summary sensors (total spools, low stock, cart count, prices, etc.)
  • Inventory sensors (total products, discrepancy counts)
  • Stocktaking sensors (last sync, scan count)
"""
from __future__ import annotations
import logging
from datetime import datetime

from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .database import (get_all_filaments, get_last_price_update, get_cart,
                        get_last_inventory_sync, get_all_inventory,
                        get_inventory_discrepancies, get_scan_history)
from .filaments import build_filament_response, _effective_price

_LOGGER = logging.getLogger(__name__)
DOMAIN = "filament_stock"


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry,
                             async_add_entities: AddEntitiesCallback) -> None:
    db_path = hass.data[DOMAIN][entry.entry_id]["db_path"]

    # --- Global summary sensors ---
    summary_entities: list[SensorEntity] = [
        TotalSpoolsSensor(hass, db_path),
        LowStockSensor(hass, db_path),
        CartCountSensor(hass, db_path),
        LastPriceUpdateSensor(hass, db_path),
        FilamentsOnSaleSensor(hass, db_path),
        TotalFilamentTypesSensor(hass, db_path),
        InventoryProductCountSensor(hass, db_path),
        DiscrepancyCountSensor(hass, db_path),
        LastInventorySyncSensor(hass, db_path),
        ScanCountSensor(hass, db_path),
    ]
    async_add_entities(summary_entities, update_before_add=True)

    # --- Per-filament device entities ---
    filament_rows = await hass.async_add_executor_job(get_all_filaments, db_path)
    filament_entities = _build_filament_entities(hass, db_path, filament_rows)
    async_add_entities(filament_entities, update_before_add=True)

    # Keep a registry so we can add new entities dynamically
    all_entities: list[SensorEntity] = list(summary_entities) + list(filament_entities)
    known_skus: set[str] = {r["sku"] for r in filament_rows}

    @callback
    def _handle_filaments_updated(_event):
        for e in all_entities:
            e.async_schedule_update_ha_state(force_refresh=True)

    @callback
    def _handle_inventory_updated(_event):
        for e in all_entities:
            if isinstance(e, (InventoryProductCountSensor, DiscrepancyCountSensor,
                               LastInventorySyncSensor, ScanCountSensor)):
                e.async_schedule_update_ha_state(force_refresh=True)

    async def _handle_new_filaments(_event):
        """Detect new filaments and add entities for them."""
        nonlocal known_skus
        rows = await hass.async_add_executor_job(get_all_filaments, db_path)
        new_rows = [r for r in rows if r["sku"] not in known_skus]
        if new_rows:
            new_entities = _build_filament_entities(hass, db_path, new_rows)
            async_add_entities(new_entities, update_before_add=True)
            all_entities.extend(new_entities)
            known_skus.update(r["sku"] for r in new_rows)

    hass.bus.async_listen(f"{DOMAIN}_filaments_updated", _handle_filaments_updated)
    hass.bus.async_listen(f"{DOMAIN}_filaments_updated", _handle_new_filaments)
    hass.bus.async_listen(f"{DOMAIN}_prices_updated",    _handle_filaments_updated)
    hass.bus.async_listen(f"{DOMAIN}_inventory_updated", _handle_inventory_updated)


def _build_filament_entities(hass, db_path, rows) -> list:
    entities = []
    for row in rows:
        sku = row["sku"]
        entities.append(FilamentSpoolsSensor(hass, db_path, sku))
        entities.append(FilamentPriceSensor(hass, db_path, sku))
        entities.append(FilamentCartSensor(hass, db_path, sku))
    return entities


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _filament_device_info(row: dict) -> DeviceInfo:
    """One HA device per filament spool type."""
    name = " ".join(filter(None, [row.get("colour"), row.get("material"), row.get("brand")]))
    return DeviceInfo(
        identifiers={(DOMAIN, f"filament_{row['sku']}")},
        name=name or row["sku"],
        model=f"{row.get('material','?')} — {row.get('spool_weight') or '?'}g",
        manufacturer=row.get("brand", "Unknown"),
        configuration_url=row.get("url"),
    )


class _FilamentBase(SensorEntity):
    _attr_should_poll = False

    def __init__(self, hass, db_path, sku):
        self._hass   = hass
        self._db     = db_path
        self._sku    = sku
        self._row: dict = {}

    async def async_update(self):
        rows = await self._hass.async_add_executor_job(get_all_filaments, self._db)
        for r in rows:
            if r["sku"] == self._sku:
                self._row = r
                break
        self._refresh()

    def _refresh(self):
        pass   # override


class _SummaryBase(SensorEntity):
    _attr_should_poll = False

    def __init__(self, hass, db_path):
        self._hass = hass
        self._db   = db_path

    async def async_update(self):
        await self._hass.async_add_executor_job(self._sync_update)

    def _sync_update(self):
        raise NotImplementedError


# ─────────────────────────────────────────────────────────────────────────────
# Per-filament entities  (one device, three entities each)
# ─────────────────────────────────────────────────────────────────────────────

class FilamentSpoolsSensor(_FilamentBase):
    """Main filament entity: reports full+partial, all metadata as attributes."""

    def __init__(self, hass, db_path, sku):
        super().__init__(hass, db_path, sku)
        self._attr_unique_id   = f"filament_stock_filament_{sku}_spools"
        self._attr_icon        = "mdi:printer-3d-nozzle"
        self._attr_native_unit_of_measurement = "spools"

    def _refresh(self):
        row = self._row
        if not row:
            return
        price, is_default, is_live, on_sale = _effective_price(row)
        self._attr_device_info = _filament_device_info(row)
        self._attr_name = (
            f"{row.get('colour','')} {row.get('material','')} {row.get('brand','')}"
        ).strip() or row["sku"]
        full    = row.get("full_spools", 0) or 0
        partial = row.get("partial_spools", 0) or 0
        self._attr_native_value = full + partial
        self._attr_extra_state_attributes = {
            # Identity
            "filament_id":    row["sku"],
            "brand":          row.get("brand"),
            "colour":         row.get("colour"),
            "material":       row.get("material"),
            "spool_weight_g": row.get("spool_weight"),
            # Stock
            "full_spools":    full,
            "partial_spools": partial,
            "total_spools":   full + partial,
            # Price
            "spool_cost":           round(price, 2) if price else None,
            "price_is_live":        is_live,
            "price_is_default":     is_default,
            "on_sale":              on_sale,
            # Links
            "image_url":  row.get("image_url"),
            "store_url":  row.get("url"),
            # Cart
            "cart_listed":  row.get("listed_qty") or 0,
            "cart_ordered": row.get("ordered_qty") or 0,
            # Status
            "live_price_error": row.get("live_error"),
        }
        # State class for history graphing
        if full == 0 and partial == 0:
            self._attr_icon = "mdi:alert-circle-outline"
        elif full == 0:
            self._attr_icon = "mdi:alert-outline"
        else:
            self._attr_icon = "mdi:printer-3d-nozzle"


class FilamentPriceSensor(_FilamentBase):
    """Price sensor for a filament — useful for automations (on sale alert, etc.)."""

    def __init__(self, hass, db_path, sku):
        super().__init__(hass, db_path, sku)
        self._attr_unique_id  = f"filament_stock_filament_{sku}_price"
        self._attr_icon       = "mdi:currency-usd"
        self._attr_native_unit_of_measurement = "AUD"
        self._attr_suggested_display_precision = 2

    def _refresh(self):
        row = self._row
        if not row:
            return
        price, is_default, is_live, on_sale = _effective_price(row)
        self._attr_device_info = _filament_device_info(row)
        label = (f"{row.get('colour','')} {row.get('material','')} {row.get('brand','')}").strip()
        self._attr_name = f"{label} Price"
        self._attr_native_value = round(price, 2) if price else None
        self._attr_extra_state_attributes = {
            "filament_id":    row["sku"],
            "is_live":        is_live,
            "is_default":     is_default,
            "on_sale":        on_sale,
            "store_url":      row.get("url"),
            "stored_cost":    row.get("spool_cost"),
            "live_cost":      row.get("live_cost"),
            "live_error":     row.get("live_error"),
        }


class FilamentCartSensor(_FilamentBase):
    """Cart sensor — shows how many of this filament are in the to-order queue."""

    def __init__(self, hass, db_path, sku):
        super().__init__(hass, db_path, sku)
        self._attr_unique_id = f"filament_stock_filament_{sku}_cart"
        self._attr_icon      = "mdi:cart-outline"
        self._attr_native_unit_of_measurement = "spools"

    def _refresh(self):
        row = self._row
        if not row:
            return
        self._attr_device_info = _filament_device_info(row)
        label = (f"{row.get('colour','')} {row.get('material','')} {row.get('brand','')}").strip()
        self._attr_name = f"{label} Cart"
        self._attr_native_value = (row.get("listed_qty") or 0) + (row.get("ordered_qty") or 0)
        self._attr_extra_state_attributes = {
            "filament_id":  row["sku"],
            "listed":       row.get("listed_qty") or 0,
            "ordered":      row.get("ordered_qty") or 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Summary sensors
# ─────────────────────────────────────────────────────────────────────────────

class TotalSpoolsSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_total_spools"
    _attr_name      = "Filament Total Spools"
    _attr_icon      = "mdi:printer-3d-nozzle"
    _attr_native_unit_of_measurement = "spools"

    def _sync_update(self):
        rows = get_all_filaments(self._db)
        full    = sum(r.get("full_spools",0) or 0 for r in rows)
        partial = sum(r.get("partial_spools",0) or 0 for r in rows)
        self._attr_native_value = full + partial
        self._attr_extra_state_attributes = {
            "full_spools": full, "partial_spools": partial,
            "filament_types": len(rows),
        }


class TotalFilamentTypesSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_filament_types"
    _attr_name      = "Filament Types"
    _attr_icon      = "mdi:format-list-bulleted"
    _attr_native_unit_of_measurement = "types"

    def _sync_update(self):
        rows = get_all_filaments(self._db)
        self._attr_native_value = len(rows)
        brands = sorted(set(r.get("brand","?") for r in rows if r.get("brand")))
        mats   = sorted(set(r.get("material","?") for r in rows if r.get("material")))
        self._attr_extra_state_attributes = {"brands": brands, "materials": mats}


class LowStockSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_low_stock"
    _attr_name      = "Filament Low Stock Count"
    _attr_icon      = "mdi:alert-circle-outline"
    _attr_native_unit_of_measurement = "filaments"

    def _sync_update(self):
        rows = get_all_filaments(self._db)
        no_full     = [r for r in rows if (r.get("full_spools") or 0) == 0]
        completely  = [r for r in no_full if (r.get("partial_spools") or 0) == 0]
        self._attr_native_value = len(no_full)
        self._attr_extra_state_attributes = {
            "no_full_spools": [
                f"{r.get('colour')} {r.get('material')} ({r.get('brand')})" for r in no_full],
            "completely_out": len(completely),
            "completely_out_list": [
                f"{r.get('colour')} {r.get('material')} ({r.get('brand')})" for r in completely],
        }


class CartCountSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_cart_count"
    _attr_name      = "Filament Cart Items"
    _attr_icon      = "mdi:cart-outline"
    _attr_native_unit_of_measurement = "items"

    def _sync_update(self):
        cart    = get_cart(self._db)
        listed  = [c for c in cart if (c.get("listed_qty") or 0) > 0]
        ordered = [c for c in cart if (c.get("ordered_qty") or 0) > 0]
        self._attr_native_value = len(listed)
        self._attr_extra_state_attributes = {
            "to_order": len(listed),
            "ordered_pending_arrival": len(ordered),
            "cart_items": [{
                "id":      c["sku"],
                "label":   f"{c.get('colour')} {c.get('material')} ({c.get('brand')})",
                "listed":  c.get("listed_qty",0),
                "ordered": c.get("ordered_qty",0),
            } for c in cart],
        }


class LastPriceUpdateSensor(_SummaryBase):
    _attr_unique_id    = "filament_stock_last_price_update"
    _attr_name         = "Filament Last Price Update"
    _attr_icon         = "mdi:update"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def _sync_update(self):
        self._attr_native_value = get_last_price_update(self._db)


class FilamentsOnSaleSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_on_sale"
    _attr_name      = "Filaments On Sale"
    _attr_icon      = "mdi:tag-outline"
    _attr_native_unit_of_measurement = "filaments"

    def _sync_update(self):
        rows    = get_all_filaments(self._db)
        on_sale = [r for r in rows if r.get("on_sale")]
        self._attr_native_value = len(on_sale)
        self._attr_extra_state_attributes = {
            "on_sale": [f"{r.get('colour')} {r.get('material')} ({r.get('brand')})" for r in on_sale],
        }


class InventoryProductCountSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_inventory_product_count"
    _attr_name      = "Inventory Product Count"
    _attr_icon      = "mdi:package-variant"
    _attr_native_unit_of_measurement = "products"

    def _sync_update(self):
        items    = get_all_inventory(self._db)
        variants = sum(len(i.get("variants",[])) for i in items)
        self._attr_native_value = len(items)
        self._attr_extra_state_attributes = {
            "total_variants": variants,
            "last_sync": (get_last_inventory_sync(self._db) or {})
        }


class DiscrepancyCountSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_discrepancy_count"
    _attr_name      = "Inventory Discrepancies"
    _attr_icon      = "mdi:alert-decagram-outline"
    _attr_native_unit_of_measurement = "issues"

    def _sync_update(self):
        d = get_inventory_discrepancies(self._db)
        total = (len(d["count_discrepancies"]) + len(d["price_discrepancies"])
                 + len(d["missing_shopify"]) + len(d["missing_square"]))
        self._attr_native_value = total
        self._attr_extra_state_attributes = {
            "count_discrepancies": len(d["count_discrepancies"]),
            "price_discrepancies": len(d["price_discrepancies"]),
            "missing_shopify":     len(d["missing_shopify"]),
            "missing_square":      len(d["missing_square"]),
        }


class LastInventorySyncSensor(_SummaryBase):
    _attr_unique_id    = "filament_stock_last_inventory_sync"
    _attr_name         = "Last Inventory Sync"
    _attr_icon         = "mdi:sync"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def _sync_update(self):
        self._attr_native_value = get_last_inventory_sync(self._db)


class ScanCountSensor(_SummaryBase):
    _attr_unique_id = "filament_stock_scan_count"
    _attr_name      = "Stocktake Scan Count"
    _attr_icon      = "mdi:barcode-scan"
    _attr_native_unit_of_measurement = "scans"

    def _sync_update(self):
        history = get_scan_history(self._db, limit=10000)
        self._attr_native_value = len(history)
        if history:
            self._attr_extra_state_attributes = {
                "last_scan_sku": history[0]["sku"],
                "last_scan_at":  history[0]["scanned_at"],
            }
