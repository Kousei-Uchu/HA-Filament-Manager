"""
platform_sync.py — Shopify + Square inventory sync.

Ported from:
  - shared_scripts/platform_apis/shopify_stock.py
  - shared_scripts/platform_apis/square_stock.py
  - Stocktaking/Managers and APIs/InventoryManager.swift (processProducts, processSquareData)
  - Stocktaking/Managers and APIs/ShopifyService.swift
  - Stocktaking/Managers and APIs/SquareService.swift
"""
from __future__ import annotations
import logging
import asyncio
from datetime import datetime
from typing import Optional
import requests

_LOGGER = logging.getLogger(__name__)

SHOPIFY_API_VERSION = "2024-01"


# ─────────────────────────────────────────────────────────────────────────────
# Shopify
# ─────────────────────────────────────────────────────────────────────────────

def _shopify_graphql(shop_url: str, token: str, query: str, variables: dict = None) -> dict:
    endpoint = f"https://{shop_url}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(endpoint, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Shopify GraphQL errors: {data['errors']}")
    return data.get("data", {})


def fetch_shopify_products(shop_url: str, token: str) -> list[dict]:
    """Fetch all products with variants + inventory quantities."""
    query = """
    {
      products(first: 250) {
        edges { node {
          id title featuredImage { url }
          variants(first: 100) { edges { node {
            id sku title inventoryQuantity price image { url }
          }}}
        }}
      }
    }
    """
    data = _shopify_graphql(shop_url, token, query)
    products = []
    for edge in data.get("products", {}).get("edges", []):
        node = edge["node"]
        variants = []
        for ve in node.get("variants", {}).get("edges", []):
            vn = ve["node"]
            try:
                price = float(vn.get("price") or 0)
            except (TypeError, ValueError):
                price = 0.0
            variants.append({
                "id": vn["id"],
                "sku": vn.get("sku"),
                "title": vn.get("title", ""),
                "inventory_quantity": vn.get("inventoryQuantity", 0),
                "price": price,
                "image_url": (vn.get("image") or {}).get("url") or (node.get("featuredImage") or {}).get("url"),
            })
        products.append({
            "id": node["id"],
            "title": node["title"],
            "image_url": (node.get("featuredImage") or {}).get("url"),
            "variants": variants,
        })
    return products


def shopify_modify_stock(shop_url: str, token: str, variant_gid: str, delta: int,
                         location_gid: str = None):
    """Adjust Shopify inventory by delta (can be negative)."""
    if not location_gid:
        location_gid = _get_default_location(shop_url, token)

    # Get inventory item id
    q = """
    query($variantId: ID!, $locationId: ID!) {
      productVariant(id: $variantId) {
        inventoryItem {
          id
          inventoryLevel(locationId: $locationId) { quantities(names: ["available"]) { quantity } }
        }
      }
    }
    """
    data = _shopify_graphql(shop_url, token, q,
                            {"variantId": variant_gid, "locationId": location_gid})
    inv_item_id = data["productVariant"]["inventoryItem"]["id"]

    mutation = """
    mutation($input: InventoryAdjustQuantitiesInput!) {
      inventoryAdjustQuantities(input: $input) {
        userErrors { field message }
      }
    }
    """
    _shopify_graphql(shop_url, token, mutation, {"input": {
        "reason": "correction",
        "name": "available",
        "changes": [{"inventoryItemId": inv_item_id, "locationId": location_gid, "delta": delta}]
    }})


def _get_default_location(shop_url: str, token: str) -> str:
    q = "{ locations(first: 1) { edges { node { id } } } }"
    data = _shopify_graphql(shop_url, token, q)
    return data["locations"]["edges"][0]["node"]["id"]


def search_shopify_products(shop_url: str, token: str, query_str: str) -> list[dict]:
    q = """
    query($q: String!) {
      productVariants(first: 20, query: $q) {
        edges { node {
          id sku title price inventoryQuantity
          product { id title featuredImage { url } }
          image { url }
        }}
      }
    }
    """
    data = _shopify_graphql(shop_url, token, q, {"q": query_str})
    results = []
    for edge in data.get("productVariants", {}).get("edges", []):
        n = edge["node"]
        results.append({
            "variant_id": n["id"],
            "sku": n.get("sku"),
            "variant_title": n.get("title"),
            "price": n.get("price"),
            "inventory_quantity": n.get("inventoryQuantity"),
            "product_id": n["product"]["id"],
            "product_title": n["product"]["title"],
            "image_url": (n.get("image") or n["product"].get("featuredImage") or {}).get("url"),
        })
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Square
# ─────────────────────────────────────────────────────────────────────────────

def _square_get(token: str, path: str, params: dict = None) -> dict:
    url = f"https://connect.squareup.com/v2{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                "Square-Version": "2024-01-18"}
    resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _square_post(token: str, path: str, body: dict) -> dict:
    url = f"https://connect.squareup.com/v2{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                "Square-Version": "2024-01-18"}
    resp = requests.post(url, headers=headers, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_square_catalog(token: str) -> list[dict]:
    """Returns flat list of catalog objects (ITEM and ITEM_VARIATION)."""
    objects = []
    cursor = None
    while True:
        params = {"types": "ITEM,ITEM_VARIATION"}
        if cursor:
            params["cursor"] = cursor
        data = _square_get(token, "/catalog/list", params)
        objects.extend(data.get("objects", []))
        cursor = data.get("cursor")
        if not cursor:
            break
    return objects


def fetch_square_inventory(token: str, location_id: str, catalog_ids: list[str]) -> dict:
    """Returns {catalog_object_id: quantity} for IN_STOCK items."""
    result = {}
    for i in range(0, len(catalog_ids), 1000):
        batch = catalog_ids[i:i+1000]
        data = _square_post(token, "/inventory/counts/batch-retrieve", {
            "catalog_object_ids": batch,
            "location_ids": [location_id],
        })
        for count in data.get("counts", []):
            if count.get("state") == "IN_STOCK":
                try:
                    result[count["catalog_object_id"]] = int(float(count.get("quantity", "0")))
                except (ValueError, TypeError):
                    result[count["catalog_object_id"]] = 0
    return result


def push_physical_counts_to_square(token: str, location_id: str,
                                   counts: list[dict]) -> None:
    """
    counts = [{"catalog_object_id": str, "quantity": int}]
    Batches into groups of 100 as Square requires.
    """
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    changes = [
        {
            "type": "PHYSICAL_COUNT",
            "physical_count": {
                "catalog_object_id": c["catalog_object_id"],
                "state": "IN_STOCK",
                "quantity": str(c["quantity"]),
                "location_id": location_id,
                "occurred_at": now,
            }
        }
        for c in counts
    ]
    for i in range(0, len(changes), 100):
        batch = changes[i:i+100]
        _square_post(token, "/inventory/changes/batch-create", {"changes": batch})


async def get_square_sales_summary(token: str, location_id: str,
                                   start_date: str, end_date: str) -> dict:
    """
    Async wrapper — runs Square API calls in executor threads.
    Returns {sku: {sold, revenue, item_name, variant_name}}
    Ported from shared_scripts/platform_apis/square_stock.py.
    """
    loop = asyncio.get_event_loop()

    def _run():
        from collections import defaultdict

        def _fmt(d):
            return d if "T" in d else d + "T00:00:00Z"

        start = _fmt(start_date)
        end   = _fmt(end_date)

        sku_summary = defaultdict(lambda: {"sold": 0, "revenue": 0.0,
                                            "item_name": "", "variant_name": ""})
        item_ids = []

        # Fetch completed orders in date range
        cursor = None
        while True:
            body = {
                "location_ids": [location_id],
                "limit": 25,
                "query": {
                    "filter": {
                        "state_filter": {"states": ["COMPLETED"]},
                        "date_time_filter": {"closed_at": {"start_at": start, "end_at": end}},
                    },
                    "sort": {"sort_field": "CLOSED_AT"},
                },
            }
            if cursor:
                body["cursor"] = cursor

            data = _square_post(token, "/orders/search", body)
            for order in data.get("orders", []):
                for li in order.get("line_items", []):
                    if "catalog_object_id" not in li:
                        continue
                    oid = li["catalog_object_id"]
                    qty = int(li["quantity"])
                    rev = int(li["base_price_money"]["amount"]) * qty / 100
                    item_ids.append(oid)
                    sku_summary[oid]["sold"] += qty
                    sku_summary[oid]["revenue"] += rev

            cursor = data.get("cursor")
            if not cursor:
                break

        if not item_ids:
            return {}

        # Resolve catalog object IDs → SKU + names
        unique_ids = list(set(item_ids))
        variation_to_parent = {}

        for i in range(0, len(unique_ids), 100):
            batch = unique_ids[i:i+100]
            data = _square_post(token, "/catalog/batch-retrieve", {"object_ids": batch})
            parent_ids = set()
            for obj in data.get("objects", []):
                vid = obj["id"]
                vd = obj.get("item_variation_data", {})
                sku_summary[vid]["sku"] = vd.get("sku")
                sku_summary[vid]["variant_name"] = vd.get("name", "")
                pid = vd.get("item_id")
                if pid:
                    variation_to_parent[vid] = pid
                    parent_ids.add(pid)

            for j in range(0, len(list(parent_ids)), 100):
                pbatch = list(parent_ids)[j:j+100]
                pdata = _square_post(token, "/catalog/batch-retrieve", {"object_ids": pbatch})
                for pobj in pdata.get("objects", []):
                    pname = (pobj.get("item_data") or {}).get("name", "")
                    for vid, pid in variation_to_parent.items():
                        if pid == pobj["id"]:
                            sku_summary[vid]["item_name"] = pname

        # Re-key by SKU
        final = {}
        for oid, val in sku_summary.items():
            sku = val.get("sku")
            if not sku:
                continue
            final[sku] = {
                "sold": val["sold"],
                "revenue": val["revenue"],
                "item_name": val["item_name"],
                "variant_name": val["variant_name"],
            }
        return final

    return await loop.run_in_executor(None, _run)


# ─────────────────────────────────────────────────────────────────────────────
# Full sync: build/update the inventory_items + inventory_variants tables
# ─────────────────────────────────────────────────────────────────────────────

def sync_all_platforms(db_path: str, shop_url: str, shopify_token: str,
                       square_token: str, square_location_id: str) -> dict:
    """
    Fetches data from Shopify and Square, merges it into the SQLite
    inventory tables, and returns a summary dict.
    Mirrors InventoryManager.syncWithAllPlatforms().
    """
    from .database import (upsert_inventory_item, upsert_inventory_variant,
                            set_last_inventory_sync)

    errors = []
    shopify_count = 0
    square_count = 0

    # ── Shopify ──────────────────────────────────────────────────────────
    if shop_url and shopify_token:
        try:
            products = fetch_shopify_products(shop_url, shopify_token)
            for product in products:
                upsert_inventory_item(db_path, product["id"], product["title"],
                                      image_url=product.get("image_url"))
                for v in product["variants"]:
                    if not v.get("sku"):
                        continue
                    upsert_inventory_variant(
                        db_path,
                        variant_id=v["id"],
                        item_id=product["id"],
                        name=v["title"],
                        sku=v["sku"],
                        shopify_qty=v["inventory_quantity"],
                        shopify_price=v["price"],
                        image_url=v.get("image_url"),
                    )
                    shopify_count += 1
            _LOGGER.info("Synced %d Shopify variants", shopify_count)
        except Exception as e:
            errors.append(f"Shopify sync error: {e}")
            _LOGGER.error("Shopify sync error: %s", e)

    # ── Square ───────────────────────────────────────────────────────────
    if square_token and square_location_id:
        try:
            catalog = fetch_square_catalog(square_token)

            # Build lookup maps
            variation_ids = []
            var_map = {}  # variation_id → {sku, name, price, item_id}
            item_names = {}  # item_id → name

            for obj in catalog:
                if obj["type"] == "ITEM":
                    item_names[obj["id"]] = (obj.get("item_data") or {}).get("name", "Unnamed")
                elif obj["type"] == "ITEM_VARIATION":
                    vd = obj.get("item_variation_data") or {}
                    sku = vd.get("sku")
                    if not sku:
                        continue
                    price_money = vd.get("price_money") or {}
                    price = price_money.get("amount", 0) / 100
                    var_map[obj["id"]] = {
                        "sku": sku,
                        "name": vd.get("name", ""),
                        "price": price,
                        "item_id": vd.get("item_id", ""),
                    }
                    variation_ids.append(obj["id"])

            # Fetch inventory counts
            counts = fetch_square_inventory(square_token, square_location_id, variation_ids)

            for vid, info in var_map.items():
                item_id = info["item_id"]
                item_name = item_names.get(item_id, "Unknown")
                qty = counts.get(vid, 0)

                upsert_inventory_item(db_path, item_id, item_name)
                upsert_inventory_variant(
                    db_path,
                    variant_id=vid,
                    item_id=item_id,
                    name=info["name"],
                    sku=info["sku"],
                    square_qty=qty,
                    square_price=info["price"],
                )
                square_count += 1

            _LOGGER.info("Synced %d Square variants", square_count)
        except Exception as e:
            errors.append(f"Square sync error: {e}")
            _LOGGER.error("Square sync error: %s", e)

    set_last_inventory_sync(db_path)

    return {
        "shopify_variants_synced": shopify_count,
        "square_variants_synced": square_count,
        "errors": errors,
    }
