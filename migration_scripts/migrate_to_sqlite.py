#!/usr/bin/env python3
"""
migrate_to_sqlite.py
====================
Migrates ALL old JSON data files to the new SQLite database.

Handles:
  filaments.json            → filaments + filament_live_prices tables
  live_filament_info.json   → filament_live_prices table
  cart.json                 → filament_cart table
  print_info.json           → print_info_products + print_info_variants + print_info_filaments tables

Usage:
    python migrate_to_sqlite.py \\
        --filaments  shared_data_storage/filaments/filaments.json \\
        --live       shared_data_storage/filaments/live_filament_info.json \\
        --cart       stock_api/api_data_storage/cart.json \\
        --print-info shared_data_storage/prints/print_info.json \\
        --output     /config/filament_stock.db

All source arguments are optional; omit any you don't have.
The migration is idempotent — safe to run multiple times.
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Schema (mirrors database.py in the integration)
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS filaments (
    sku             TEXT PRIMARY KEY,
    brand           TEXT,
    colour          TEXT,
    material        TEXT,
    name            TEXT,
    full_spools     INTEGER NOT NULL DEFAULT 0,
    partial_spools  INTEGER NOT NULL DEFAULT 0,
    spool_cost      REAL,
    spool_weight    INTEGER,
    image_url       TEXT,
    url             TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS filament_live_prices (
    sku         TEXT PRIMARY KEY REFERENCES filaments(sku) ON DELETE CASCADE,
    spool_cost  REAL,
    on_sale     INTEGER NOT NULL DEFAULT 0,
    price_live  INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS filament_cart (
    sku         TEXT PRIMARY KEY REFERENCES filaments(sku) ON DELETE CASCADE,
    listed_qty  INTEGER NOT NULL DEFAULT 0,
    ordered_qty INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS print_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sku          TEXT,
    item_name    TEXT,
    variant_name TEXT,
    price        REAL,
    deal_text    TEXT,
    template     TEXT,
    qty          INTEGER,
    printed_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS inventory_items (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    sku         TEXT NOT NULL DEFAULT '',
    image_url   TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS inventory_variants (
    id              TEXT PRIMARY KEY,
    item_id         TEXT NOT NULL REFERENCES inventory_items(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    sku             TEXT NOT NULL,
    shopify_qty     INTEGER,
    square_qty      INTEGER,
    physical_count  INTEGER NOT NULL DEFAULT 0,
    shopify_price   REAL,
    square_price    REAL,
    image_url       TEXT,
    last_scanned    TEXT,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_variants_item ON inventory_variants(item_id);
CREATE INDEX IF NOT EXISTS idx_variants_sku  ON inventory_variants(sku);

CREATE TABLE IF NOT EXISTS scan_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sku             TEXT NOT NULL,
    change_amount   INTEGER NOT NULL,
    scanned_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS print_info_products (
    name            TEXT PRIMARY KEY,
    print_hours     INTEGER NOT NULL DEFAULT 0,
    print_minutes   INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS print_info_variants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name    TEXT NOT NULL REFERENCES print_info_products(name) ON DELETE CASCADE,
    variant_name    TEXT NOT NULL,
    sku             TEXT NOT NULL,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(product_name, sku)
);

CREATE TABLE IF NOT EXISTS print_info_filaments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    variant_id  INTEGER NOT NULL REFERENCES print_info_variants(id) ON DELETE CASCADE,
    filament_id TEXT NOT NULL,
    grams       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS app_state (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# filaments.json
# ─────────────────────────────────────────────────────────────────────────────

def migrate_filaments(conn: sqlite3.Connection, path: str) -> int:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    now = datetime.now().isoformat()
    count = 0

    for item in data:
        sku = item.get("sku")
        if not sku or sku == "misc":
            continue

        # Old schema used "image", new uses "image_url"
        image_url = item.get("image_url") or item.get("image")

        conn.execute("""
            INSERT INTO filaments
                (sku, brand, colour, material, name,
                 full_spools, partial_spools, spool_cost, spool_weight,
                 image_url, url, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(sku) DO UPDATE SET
                brand=excluded.brand, colour=excluded.colour, material=excluded.material,
                name=excluded.name, full_spools=excluded.full_spools,
                partial_spools=excluded.partial_spools, spool_cost=excluded.spool_cost,
                spool_weight=excluded.spool_weight, image_url=excluded.image_url,
                url=excluded.url, updated_at=excluded.updated_at
        """, (
            sku, item.get("brand"),
            item.get("colour") or item.get("color"),
            item.get("material"), item.get("name"),
            int(item.get("full_spools", 0)),
            int(item.get("partial_spools", 0)),
            item.get("spool_cost"), item.get("spool_weight"),
            image_url, item.get("url"), now, now,
        ))
        count += 1

    conn.commit()
    return count


# ─────────────────────────────────────────────────────────────────────────────
# live_filament_info.json
# ─────────────────────────────────────────────────────────────────────────────

def migrate_live_prices(conn: sqlite3.Connection, path: str) -> int:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    now = datetime.now().isoformat()
    count = 0

    for sku, info in data.items():
        if sku == "last_update":
            try:
                conn.execute("""
                    INSERT INTO app_state(key,value) VALUES('last_price_update',?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """, (info,))
            except Exception:
                pass
            continue

        if not isinstance(info, dict):
            continue

        exists = conn.execute(
            "SELECT sku FROM filaments WHERE sku=?", (sku,)).fetchone()
        if not exists:
            print(f"  ⚠  Skipping live price for unknown SKU '{sku}'")
            continue

        conn.execute("""
            INSERT INTO filament_live_prices(sku,spool_cost,on_sale,price_live,error,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(sku) DO UPDATE SET spool_cost=excluded.spool_cost,
              on_sale=excluded.on_sale, price_live=excluded.price_live,
              error=excluded.error, updated_at=excluded.updated_at
        """, (
            sku, info.get("spool_cost"),
            int(bool(info.get("on_sale", False))),
            int(bool(info.get("live", False))),
            info.get("error"), now,
        ))
        count += 1

    conn.commit()
    return count


# ─────────────────────────────────────────────────────────────────────────────
# cart.json
# ─────────────────────────────────────────────────────────────────────────────

def migrate_cart(conn: sqlite3.Connection, path: str) -> int:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        print("  ⚠  cart.json: unexpected format (expected list), skipping")
        return 0

    now = datetime.now().isoformat()
    count = 0

    for item in data:
        sku = item.get("id") or item.get("sku")
        if not sku:
            continue

        exists = conn.execute(
            "SELECT sku FROM filaments WHERE sku=?", (sku,)).fetchone()
        if not exists:
            print(f"  ⚠  Skipping cart entry for unknown SKU '{sku}'")
            continue

        conn.execute("""
            INSERT INTO filament_cart(sku,listed_qty,ordered_qty,updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(sku) DO UPDATE SET listed_qty=excluded.listed_qty,
              ordered_qty=excluded.ordered_qty, updated_at=excluded.updated_at
        """, (
            sku,
            int(item.get("listed_qty", 0)),
            int(item.get("ordered_qty", 0)),
            now,
        ))
        count += 1

    conn.commit()
    return count


# ─────────────────────────────────────────────────────────────────────────────
# print_info.json
# ─────────────────────────────────────────────────────────────────────────────

def migrate_print_info(conn: sqlite3.Connection, path: str) -> tuple[int, int]:
    """
    print_info.json format:
    {
      "Product Name": {
        "name": "Product Name",
        "print_hours": 2,
        "print_minutes": 30,
        "variants": [
          {"variant_name": "Blue", "sku": "Dragon-Blue", "filaments": [{"id": "bambu-pla-blue", "grams": 45.2}]}
        ]
      }
    }
    Returns (products_count, variants_count).
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    now = datetime.now().isoformat()
    products = 0
    variants = 0

    for product_key, product in data.items():
        name = product.get("name") or product_key
        hours = int(product.get("print_hours", 0))
        minutes = int(product.get("print_minutes", 0))

        conn.execute("""
            INSERT INTO print_info_products(name,print_hours,print_minutes,updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET print_hours=excluded.print_hours,
              print_minutes=excluded.print_minutes, updated_at=excluded.updated_at
        """, (name, hours, minutes, now))
        products += 1

        for v in product.get("variants", []):
            sku = v.get("sku", "")
            vname = v.get("variant_name", "")
            filaments = v.get("filaments", [])

            conn.execute("""
                INSERT INTO print_info_variants(product_name,variant_name,sku,updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(product_name,sku) DO UPDATE SET
                  variant_name=excluded.variant_name, updated_at=excluded.updated_at
            """, (name, vname, sku, now))

            vid_row = conn.execute(
                "SELECT id FROM print_info_variants WHERE product_name=? AND sku=?",
                (name, sku)).fetchone()

            if vid_row:
                vid = vid_row["id"]
                conn.execute(
                    "DELETE FROM print_info_filaments WHERE variant_id=?", (vid,))
                for f in filaments:
                    conn.execute(
                        "INSERT INTO print_info_filaments(variant_id,filament_id,grams) VALUES(?,?,?)",
                        (vid, f["id"], float(f["grams"])))
                variants += 1

    conn.commit()
    return products, variants


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Migrate Filament Stock JSON data files to SQLite.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--filaments",   metavar="PATH", help="Path to filaments.json")
    p.add_argument("--live",        metavar="PATH", help="Path to live_filament_info.json")
    p.add_argument("--cart",        metavar="PATH", help="Path to cart.json")
    p.add_argument("--print-info",  metavar="PATH", dest="print_info",
                   help="Path to print_info.json")
    p.add_argument("--output",      metavar="PATH", required=True,
                   help="Output SQLite DB path (e.g. /config/filament_stock.db)")
    args = p.parse_args()

    print(f"\n📦  Filament Stock — JSON → SQLite Migration")
    print(f"    Output: {args.output}\n")

    conn = get_conn(args.output)
    init_schema(conn)

    if args.filaments:
        if not os.path.isfile(args.filaments):
            print(f"✗ Not found: {args.filaments}", file=sys.stderr); sys.exit(1)
        n = migrate_filaments(conn, args.filaments)
        print(f"✓ Filaments:           {n:>4} records")
    else:
        print("  (skipping filaments.json)")

    if args.live:
        if not os.path.isfile(args.live):
            print(f"✗ Not found: {args.live}", file=sys.stderr); sys.exit(1)
        n = migrate_live_prices(conn, args.live)
        print(f"✓ Live prices:         {n:>4} records")
    else:
        print("  (skipping live_filament_info.json)")

    if args.cart:
        if not os.path.isfile(args.cart):
            print(f"✗ Not found: {args.cart}", file=sys.stderr); sys.exit(1)
        n = migrate_cart(conn, args.cart)
        print(f"✓ Cart:                {n:>4} records")
    else:
        print("  (skipping cart.json)")

    if args.print_info:
        if not os.path.isfile(args.print_info):
            print(f"✗ Not found: {args.print_info}", file=sys.stderr); sys.exit(1)
        np_, nv = migrate_print_info(conn, args.print_info)
        print(f"✓ Print info:          {np_:>4} products, {nv} variants")
    else:
        print("  (skipping print_info.json)")

    conn.close()
    print(f"\n🎉  Migration complete → {args.output}\n")
    print("Next steps:")
    print("  1. Copy filament_stock.db to your HA /config/ directory")
    print("  2. Install the integration via HACS or by copying custom_components/")
    print("  3. Restart Home Assistant")
    print("  4. Add the integration in Settings → Devices & Services\n")


if __name__ == "__main__":
    main()
