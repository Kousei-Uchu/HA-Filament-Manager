"""filament_stock — Home Assistant integration."""
from __future__ import annotations
import logging
import os
import asyncio
from datetime import datetime, timedelta

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.event import async_track_time_change
from homeassistant.components.http import HomeAssistantView
from homeassistant.helpers import config_validation as cv
from aiohttp import web

from .database import (init_db, get_db_path, get_last_price_update,
                        get_last_inventory_sync, get_scan_history, get_print_jobs,
                        update_physical_count_by_sku, reset_all_physical_counts,
                        undo_scan, cleanup_inventory, get_inventory_discrepancies,
                        get_all_inventory, get_all_filaments, get_print_info_for_sku,
                        upsert_print_info_product, get_all_print_info)
from .filaments import (get_all, get_info, get_cart, new_spool, open_spool, finish_spool,
                         set_spool, modify_spool, delete_spool, add_to_cart, mark_ordered,
                         mark_arrived, resolve_print_params)
from .filament_scraper import refresh_all_prices
from .cost_calculator import get_cogs
from .platform_sync import sync_all_platforms, get_square_sales_summary

_LOGGER = logging.getLogger(__name__)
DOMAIN = "filament_stock"
PLATFORMS = ["sensor"]


# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})
    db_path = get_db_path(hass.config.config_dir)
    await hass.async_add_executor_job(init_db, db_path)

    cfg = {**entry.data, **entry.options}
    tpaths = {k: cfg.get(f"{k}_label_path","")
              for k in ("tag","sticker","egg","small_sticker")}

    hass.data[DOMAIN][entry.entry_id] = {"db_path": db_path, "template_paths": tpaths, "config": cfg}

    static_path = os.path.join(os.path.dirname(__file__), "www")

    # Register static files under /filament_stock_panel
    await hass.http.async_register_static_paths(
        url_path="/filament_stock_panel",
        file_path=static_path,
        cache_headers=False,
    )

    # Register panel
    try:
        hass.components.frontend.async_register_built_in_panel(
            component_name="iframe",
            sidebar_title="Filament Stock",
            sidebar_icon="mdi:printer-3d-nozzle",
            frontend_url_path="filament_stock",
            config={"url": "/filament_stock_panel/panel.html"},
        )
    except Exception as e:
        _LOGGER.warning("Panel registration failed: %s", e)

    # HTTP views
    views = [
        FilamentListView, FilamentOpenView, FilamentFinishView, FilamentDeleteView,
        FilamentNewView, FilamentSetView, FilamentModifyView,
        FilamentCartListView, FilamentCartAddView, FilamentCartOrderedView, FilamentCartArrivedView,
        StockAddView,
        InventoryListView, InventorySyncView, InventoryScanView, InventoryResetView,
        InventoryUndoScanView, InventoryCleanupView, InventoryDiscrepancyView,
        InventoryPushSquareView,
        ScanHistoryView, PrintHistoryView,
        ReportView, CogsView,
        PrintInfoListView, PrintInfoUpsertView,
    ]
    for V in views:
        try:
            hass.http.register_view(V(hass, db_path, cfg, tpaths))
        except Exception as e:
            _LOGGER.warning("Failed to register view %s: %s", V.__name__, e)

    _register_services(hass, db_path, cfg, tpaths)
    _schedule_tasks(hass, db_path, cfg)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_reload))
    return True


async def _async_reload(hass, entry):
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# Scheduling
# ─────────────────────────────────────────────────────────────────────────────

def _schedule_tasks(hass: HomeAssistant, db_path: str, cfg: dict):
    refresh_hour = int(cfg.get("price_refresh_hour", 3))

    async def _do_price_refresh(_=None):
        _LOGGER.info("Running scheduled price refresh")
        await hass.async_add_executor_job(refresh_all_prices, db_path)
        hass.bus.async_fire(f"{DOMAIN}_prices_updated")

    async def _do_inventory_sync(_=None):
        _LOGGER.info("Running scheduled inventory sync")
        await hass.async_add_executor_job(
            sync_all_platforms, db_path,
            cfg.get("shopify_url",""), cfg.get("shopify_token",""),
            cfg.get("square_token",""), cfg.get("square_location_id",""),
        )
        hass.bus.async_fire(f"{DOMAIN}_inventory_updated")

    async def _startup(_=None):
        last_price = await hass.async_add_executor_job(get_last_price_update, db_path)
        if not last_price or datetime.now() - last_price > timedelta(hours=24):
            await _do_price_refresh()
        last_inv = await hass.async_add_executor_job(get_last_inventory_sync, db_path)
        if not last_inv or datetime.now() - last_inv > timedelta(hours=1):
            await _do_inventory_sync()

    async_track_time_change(hass, _do_price_refresh,     hour=refresh_hour, minute=0, second=0)
    async_track_time_change(hass, _do_inventory_sync,    hour=0,  minute=0,  second=0)
    hass.bus.async_listen_once("homeassistant_started", _startup)


# ─────────────────────────────────────────────────────────────────────────────
# HA Services
# ─────────────────────────────────────────────────────────────────────────────

def _register_services(hass, db_path, cfg, tpaths):
    def _fire(event):
        hass.bus.async_fire(f"{DOMAIN}_{event}")

    async def svc_open(call):
        await hass.async_add_executor_job(open_spool, db_path, call.data["filament_id"])
        _fire("filaments_updated")

    async def svc_finish(call):
        await hass.async_add_executor_job(finish_spool, db_path, call.data["filament_id"])
        _fire("filaments_updated")

    async def svc_new(call):
        await hass.async_add_executor_job(
            new_spool, db_path, call.data.get("filament_id"),
            call.data.get("full_rolls",0), call.data.get("partial_rolls",0),
            call.data.get("brand"), call.data.get("colour"), call.data.get("material"),
            call.data.get("spool_cost"), call.data.get("spool_weight"),
            call.data.get("image_url"), call.data.get("url"),
        )
        _fire("filaments_updated")

    async def svc_refresh_prices(call):
        await hass.async_add_executor_job(refresh_all_prices, db_path)
        _fire("prices_updated")

    async def svc_sync_inventory(call):
        await hass.async_add_executor_job(
            sync_all_platforms, db_path,
            cfg.get("shopify_url",""), cfg.get("shopify_token",""),
            cfg.get("square_token",""), cfg.get("square_location_id",""),
        )
        _fire("inventory_updated")

    async def svc_scan(call):
        sku    = call.data["sku"]
        delta  = call.data.get("change_amount", 1)
        await hass.async_add_executor_job(update_physical_count_by_sku, db_path, sku, delta)
        _fire("inventory_updated")

    async def svc_reset_counts(call):
        await hass.async_add_executor_job(reset_all_physical_counts, db_path)
        _fire("inventory_updated")

    async def svc_push_square(call):
        from .database import get_all_inventory as _gai
        from .platform_sync import push_physical_counts_to_square, fetch_square_catalog
        items = await hass.async_add_executor_job(_gai, db_path)
        catalog = await hass.async_add_executor_job(
            fetch_square_catalog, cfg.get("square_token",""))
        sku_to_catid = {}
        for obj in catalog:
            if obj["type"] == "ITEM_VARIATION":
                vd = obj.get("item_variation_data") or {}
                sk = vd.get("sku")
                if sk:
                    sku_to_catid[sk] = obj["id"]
        counts = []
        for item in items:
            for v in item.get("variants", []):
                cid = sku_to_catid.get(v["sku"])
                if cid:
                    counts.append({"catalog_object_id": cid, "quantity": v["physical_count"]})
        if counts:
            await hass.async_add_executor_job(
                push_physical_counts_to_square,
                cfg.get("square_token",""), cfg.get("square_location_id",""), counts,
            )

    reg = hass.services.async_register
    reg(DOMAIN, "open_spool",       svc_open,           vol.Schema({vol.Required("filament_id"): str}))
    reg(DOMAIN, "finish_spool",     svc_finish,         vol.Schema({vol.Required("filament_id"): str}))
    reg(DOMAIN, "new_spool",        svc_new,            vol.Schema({
        vol.Optional("filament_id"): str, vol.Optional("full_rolls",default=0): int,
        vol.Optional("partial_rolls",default=0): int, vol.Optional("brand"): str,
        vol.Optional("colour"): str, vol.Optional("material"): str,
        vol.Optional("spool_cost"): vol.Coerce(float), vol.Optional("spool_weight"): int,
        vol.Optional("image_url"): str, vol.Optional("url"): str,
    }))
    reg(DOMAIN, "refresh_prices",   svc_refresh_prices, vol.Schema({}))
    reg(DOMAIN, "sync_inventory",   svc_sync_inventory, vol.Schema({}))
    reg(DOMAIN, "scan_item",        svc_scan,           vol.Schema({
        vol.Required("sku"): str, vol.Optional("change_amount",default=1): int,
    }))
    reg(DOMAIN, "reset_counts",     svc_reset_counts,   vol.Schema({}))
    reg(DOMAIN, "push_to_square",   svc_push_square,    vol.Schema({}))


# ─────────────────────────────────────────────────────────────────────────────
# Base view helper
# ─────────────────────────────────────────────────────────────────────────────

class _BaseView(HomeAssistantView):
    requires_auth = True
    def __init__(self, hass, db_path, cfg, tpaths):
        self._hass  = hass
        self._db    = db_path
        self._cfg   = cfg
        self._tp    = tpaths

    def _fire(self, event):
        self._hass.bus.async_fire(f"{DOMAIN}_{event}")

    async def _json_body(self, request):
        try:
            return await request.json()
        except Exception:
            return {}


# ─────────────────────────────────────────────────────────────────────────────
# Filament views
# ─────────────────────────────────────────────────────────────────────────────

class FilamentListView(_BaseView):
    url  = "/api/filament_stock/filaments/list"
    name = "api:filament_stock:filaments_list"
    async def get(self, request):
        filaments, err = await self._hass.async_add_executor_job(get_all, self._db)
        if err: return self.json({"error": err}, status_code=500)
        info = await self._hass.async_add_executor_job(get_info, self._db)
        return self.json({"filaments": filaments, "info": info})

class FilamentOpenView(_BaseView):
    url  = "/api/filament_stock/filaments/open/{filament_id}"
    name = "api:filament_stock:filaments_open"
    async def post(self, request, filament_id):
        ok, err = await self._hass.async_add_executor_job(open_spool, self._db, filament_id)
        self._fire("filaments_updated")
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class FilamentFinishView(_BaseView):
    url  = "/api/filament_stock/filaments/finish/{filament_id}"
    name = "api:filament_stock:filaments_finish"
    async def post(self, request, filament_id):
        ok, err = await self._hass.async_add_executor_job(finish_spool, self._db, filament_id)
        self._fire("filaments_updated")
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class FilamentDeleteView(_BaseView):
    url  = "/api/filament_stock/filaments/delete/{filament_id}"
    name = "api:filament_stock:filaments_delete"
    async def post(self, request, filament_id):
        ok, err = await self._hass.async_add_executor_job(delete_spool, self._db, filament_id)
        self._fire("filaments_updated")
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class FilamentNewView(_BaseView):
    url  = "/api/filament_stock/filaments/new"
    name = "api:filament_stock:filaments_new"
    async def post(self, request):
        r = await self._json_body(request)
        ok, err = await self._hass.async_add_executor_job(
            new_spool, self._db, r.get("filament_id"), r.get("full_rolls",0),
            r.get("partial_rolls",0), r.get("brand"), r.get("colour"), r.get("material"),
            r.get("spool_cost"), r.get("spool_weight"), r.get("image_url"), r.get("url"))
        self._fire("filaments_updated")
        if not ok: return self.json({"error": err}, status_code=400)
        return self.json({"warning": err, "status":"ok"} if err else {"status":"ok"})

class FilamentSetView(_BaseView):
    url  = "/api/filament_stock/filaments/set"
    name = "api:filament_stock:filaments_set"
    async def post(self, request):
        r = await self._json_body(request)
        ok, err = await self._hass.async_add_executor_job(
            set_spool, self._db, r.get("filament_id"), r.get("full_rolls",0),
            r.get("partial_rolls",0), r.get("brand"), r.get("colour"), r.get("material"),
            r.get("spool_cost"), r.get("spool_weight"), r.get("image_url"), r.get("url"))
        self._fire("filaments_updated")
        if not ok: return self.json({"error": err}, status_code=400)
        return self.json({"warning": err, "status":"ok"} if err else {"status":"ok"})

class FilamentModifyView(_BaseView):
    url  = "/api/filament_stock/filaments/modify/{filament_id}"
    name = "api:filament_stock:filaments_modify"
    async def post(self, request, filament_id):
        r = await self._json_body(request)
        ok, err = await self._hass.async_add_executor_job(
            modify_spool, self._db, filament_id,
            r.get("brand"), r.get("colour"), r.get("material"),
            r.get("spool_cost"), r.get("spool_weight"), r.get("image_url"), r.get("url"))
        self._fire("filaments_updated")
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class FilamentCartListView(_BaseView):
    url  = "/api/filament_stock/filaments/cart"
    name = "api:filament_stock:filaments_cart_list"
    async def get(self, request):
        cart = await self._hass.async_add_executor_job(get_cart, self._db)
        return self.json({"cart": cart})

class FilamentCartAddView(_BaseView):
    url  = "/api/filament_stock/filaments/cart/add"
    name = "api:filament_stock:filaments_cart_add"
    async def post(self, request):
        r = await self._json_body(request)
        ok, err = await self._hass.async_add_executor_job(
            add_to_cart, self._db, r.get("filament_id"), r.get("qty",0))
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class FilamentCartOrderedView(_BaseView):
    url  = "/api/filament_stock/filaments/cart/ordered"
    name = "api:filament_stock:filaments_cart_ordered"
    async def post(self, request):
        r = await self._json_body(request)
        ok, err = await self._hass.async_add_executor_job(
            mark_ordered, self._db, r.get("filament_id"), r.get("qty",0))
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class FilamentCartArrivedView(_BaseView):
    url  = "/api/filament_stock/filaments/cart/arrived"
    name = "api:filament_stock:filaments_cart_arrived"
    async def post(self, request):
        r = await self._json_body(request)
        ok, err = await self._hass.async_add_executor_job(
            mark_arrived, self._db, r.get("filament_id"), r.get("qty",0))
        self._fire("filaments_updated")
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class StockAddView(_BaseView):
    url  = "/api/filament_stock/stock/add"
    name = "api:filament_stock:stock_add"
    async def post(self, request):
        r = await self._json_body(request)
        params, err = await self._hass.async_add_executor_job(
            resolve_print_params, r.get("qty",1), r.get("sku",""),
            r.get("price",0.0), r.get("item_name",""), r.get("variant_name",""),
            r.get("sticker_key"), r.get("deal_key"), self._tp)
        if err:
            return self.json(err, status_code=422)
        await self._hass.services.async_call("brother_ql_print","print_label",{
            "template": params["template"],
            "quantity": params["quantity"],
            "fields":   params["fields"],
        }, blocking=True)
        shopify_url = self._cfg.get("shopify_url","")
        shopify_tok = self._cfg.get("shopify_token","")
        variant_gid = r.get("variant_gid","")
        if shopify_url and shopify_tok and variant_gid:
            from .platform_sync import shopify_modify_stock
            await self._hass.async_add_executor_job(
                shopify_modify_stock, shopify_url, shopify_tok, variant_gid, r.get("qty",1))
        return self.json({"status":"ok"})


# ─────────────────────────────────────────────────────────────────────────────
# Inventory / Stocktaking views
# ─────────────────────────────────────────────────────────────────────────────

class InventoryListView(_BaseView):
    url  = "/api/filament_stock/inventory/list"
    name = "api:filament_stock:inventory_list"
    async def get(self, request):
        items = await self._hass.async_add_executor_job(get_all_inventory, self._db)
        return self.json({"items": items})

class InventorySyncView(_BaseView):
    url  = "/api/filament_stock/inventory/sync"
    name = "api:filament_stock:inventory_sync"
    async def post(self, request):
        result = await self._hass.async_add_executor_job(
            sync_all_platforms, self._db,
            self._cfg.get("shopify_url",""), self._cfg.get("shopify_token",""),
            self._cfg.get("square_token",""), self._cfg.get("square_location_id",""))
        self._fire("inventory_updated")
        return self.json(result)

class InventoryScanView(_BaseView):
    url  = "/api/filament_stock/inventory/scan"
    name = "api:filament_stock:inventory_scan"
    async def post(self, request):
        r = await self._json_body(request)
        sku   = r.get("sku","")
        delta = r.get("change_amount", 1)
        ok, err = await self._hass.async_add_executor_job(
            update_physical_count_by_sku, self._db, sku, delta)
        self._fire("inventory_updated")
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class InventoryResetView(_BaseView):
    url  = "/api/filament_stock/inventory/reset"
    name = "api:filament_stock:inventory_reset"
    async def post(self, request):
        await self._hass.async_add_executor_job(reset_all_physical_counts, self._db)
        self._fire("inventory_updated")
        return self.json({"status":"ok"})

class InventoryUndoScanView(_BaseView):
    url  = "/api/filament_stock/inventory/undo/{scan_id}"
    name = "api:filament_stock:inventory_undo"
    async def post(self, request, scan_id):
        ok, err = await self._hass.async_add_executor_job(undo_scan, self._db, int(scan_id))
        self._fire("inventory_updated")
        return self.json({"error": err} if not ok else {"status":"ok"}, status_code=400 if not ok else 200)

class InventoryCleanupView(_BaseView):
    url  = "/api/filament_stock/inventory/cleanup"
    name = "api:filament_stock:inventory_cleanup"
    async def post(self, request):
        removed = await self._hass.async_add_executor_job(cleanup_inventory, self._db)
        self._fire("inventory_updated")
        return self.json({"removed": removed, "status":"ok"})

class InventoryDiscrepancyView(_BaseView):
    url  = "/api/filament_stock/inventory/discrepancies"
    name = "api:filament_stock:inventory_discrepancies"
    async def get(self, request):
        data = await self._hass.async_add_executor_job(get_inventory_discrepancies, self._db)
        return self.json(data)

class InventoryPushSquareView(_BaseView):
    url  = "/api/filament_stock/inventory/push_square"
    name = "api:filament_stock:inventory_push_square"
    async def post(self, request):
        await self._hass.services.async_call(DOMAIN, "push_to_square", {}, blocking=True)
        return self.json({"status":"ok"})


# ─────────────────────────────────────────────────────────────────────────────
# History views
# ─────────────────────────────────────────────────────────────────────────────

class ScanHistoryView(_BaseView):
    url  = "/api/filament_stock/history/scans"
    name = "api:filament_stock:scan_history"
    async def get(self, request):
        limit = int(request.rel_url.query.get("limit", 200))
        rows  = await self._hass.async_add_executor_job(get_scan_history, self._db, limit)
        return self.json({"history": rows})

class PrintHistoryView(_BaseView):
    url  = "/api/filament_stock/history/prints"
    name = "api:filament_stock:print_history"
    async def get(self, request):
        limit = int(request.rel_url.query.get("limit", 100))
        rows  = await self._hass.async_add_executor_job(get_print_jobs, self._db, limit)
        return self.json({"history": rows})


# ─────────────────────────────────────────────────────────────────────────────
# Reports
# ─────────────────────────────────────────────────────────────────────────────

class ReportView(_BaseView):
    url  = "/api/filament_stock/reports/profit"
    name = "api:filament_stock:report_profit"
    async def get(self, request):
        start = request.rel_url.query.get("start_date")
        end   = request.rel_url.query.get("end_date")
        if not start or not end:
            return self.json({"error": "start_date and end_date required"}, status_code=400)
        sq_token = self._cfg.get("square_token","")
        sq_loc   = self._cfg.get("square_location_id","")
        if not sq_token or not sq_loc:
            return self.json({"error": "Square not configured"}, status_code=400)

        sales = await get_square_sales_summary(sq_token, sq_loc, start, end)
        db = self._db
        items = {}
        total_rev = total_cogs = total_inf_cogs = total_profit = 0.0

        for sku, data in sales.items():
            if data["sold"] == 0:
                continue
            cogs = await self._hass.async_add_executor_job(get_cogs, db, sku)
            if cogs is None:
                continue
            cogs_inf     = cogs * 1.25
            sold         = data["sold"]
            revenue      = data["revenue"]
            total_cost   = sold * cogs_inf
            total_pr     = revenue - total_cost
            pm           = (total_pr / revenue * 100) if revenue > 0 else 0
            items[sku]   = {
                "name":             f"{data['item_name']} | {data['variant_name']}",
                "sold":             sold,
                "revenue":          round(revenue, 2),
                "cogs_per_item":    cogs,
                "cogs_safety_margin": round(cogs_inf / cogs, 4),
                "total_cost":       round(total_cost, 2),
                "total_profit":     round(total_pr, 2),
                "profit_margin":    round(pm, 2),
            }
            total_rev       += revenue
            total_cogs      += sold * cogs
            total_inf_cogs  += sold * cogs_inf
            total_profit    += total_pr

        avg_margin = (total_profit / total_rev * 100) if total_rev > 0 else 0
        return self.json({
            "items": items,
            "summary": {
                "total_revenue":       round(total_rev, 2),
                "total_cogs":          round(total_cogs, 2),
                "total_inflated_cogs": round(total_inf_cogs, 2),
                "total_profit":        round(total_profit, 2),
                "avg_profit_margin":   round(avg_margin, 2),
            }
        })


class CogsView(_BaseView):
    url  = "/api/filament_stock/cogs"
    name = "api:filament_stock:cogs"
    async def get(self, request):
        sku = request.rel_url.query.get("sku","")
        if not sku:
            return self.json({"error":"sku required"}, status_code=400)
        cogs = await self._hass.async_add_executor_job(get_cogs, self._db, sku)
        if cogs is None:
            return self.json({"error":"SKU not in print info"}, status_code=404)
        return self.json({"sku": sku, "cogs": cogs, "inflated_cogs": round(cogs * 1.25, 2)})


# ─────────────────────────────────────────────────────────────────────────────
# Print info
# ─────────────────────────────────────────────────────────────────────────────

class PrintInfoListView(_BaseView):
    url  = "/api/filament_stock/print_info/list"
    name = "api:filament_stock:print_info_list"
    async def get(self, request):
        data = await self._hass.async_add_executor_job(get_all_print_info, self._db)
        return self.json({"products": data})

class PrintInfoUpsertView(_BaseView):
    url  = "/api/filament_stock/print_info/upsert"
    name = "api:filament_stock:print_info_upsert"
    async def post(self, request):
        r = await self._json_body(request)
        name = r.get("name")
        if not name:
            return self.json({"error":"name required"}, status_code=400)
        await self._hass.async_add_executor_job(
            upsert_print_info_product, self._db,
            name, r.get("print_hours",0), r.get("print_minutes",0), r.get("variants",[]))
        return self.json({"status":"ok"})
