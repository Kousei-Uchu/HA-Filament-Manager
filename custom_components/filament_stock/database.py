"""
database.py — SQLite storage layer for Filament Stock Manager.

Tables:
  filaments              — permanent spool data
  filament_live_prices   — scraped live pricing
  filament_cart          — ordering queue
  print_jobs             — print history log
  inventory_items        — stocktaking products (Shopify + Square)
  inventory_variants     — product variants with per-platform counts
  scan_history           — barcode scan audit log
  print_info             — print cost info (filament usage + print times)
  app_state              — key/value metadata (last sync times, etc.)
"""
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

DB_FILENAME = "filament_stock.db"


def get_db_path(config_dir: str) -> str:
    return str(Path(config_dir) / DB_FILENAME)


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str):
    conn = get_connection(db_path)
    with conn:
        conn.executescript("""
            -- ── Filaments ──────────────────────────────────────────────────
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

            -- ── Inventory / Stocktaking ────────────────────────────────────
            CREATE TABLE IF NOT EXISTS inventory_items (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                sku         TEXT NOT NULL DEFAULT '',
                image_url   TEXT,
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS inventory_variants (
                id                  TEXT PRIMARY KEY,
                item_id             TEXT NOT NULL REFERENCES inventory_items(id) ON DELETE CASCADE,
                name                TEXT NOT NULL,
                sku                 TEXT NOT NULL,
                shopify_qty         INTEGER,
                square_qty          INTEGER,
                physical_count      INTEGER NOT NULL DEFAULT 0,
                shopify_price       REAL,
                square_price        REAL,
                image_url           TEXT,
                last_scanned        TEXT,
                updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_variants_item ON inventory_variants(item_id);
            CREATE INDEX IF NOT EXISTS idx_variants_sku  ON inventory_variants(sku);

            CREATE TABLE IF NOT EXISTS scan_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                sku             TEXT NOT NULL,
                change_amount   INTEGER NOT NULL,
                scanned_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            -- ── Print cost info ────────────────────────────────────────────
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

            -- ── Misc state ─────────────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS app_state (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# App state helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_state(db_path: str, key: str) -> Optional[str]:
    conn = get_connection(db_path)
    try:
        row = conn.execute("SELECT value FROM app_state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None
    finally:
        conn.close()


def set_state(db_path: str, key: str, value: str):
    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO app_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        )
        conn.commit()
    finally:
        conn.close()


def get_last_price_update(db_path: str) -> Optional[datetime]:
    v = get_state(db_path, "last_price_update")
    return datetime.fromisoformat(v) if v else None


def set_last_price_update(db_path: str):
    set_state(db_path, "last_price_update", datetime.now().isoformat())


def get_last_inventory_sync(db_path: str) -> Optional[datetime]:
    v = get_state(db_path, "last_inventory_sync")
    return datetime.fromisoformat(v) if v else None


def set_last_inventory_sync(db_path: str):
    set_state(db_path, "last_inventory_sync", datetime.now().isoformat())


# ─────────────────────────────────────────────────────────────────────────────
# Filament CRUD
# ─────────────────────────────────────────────────────────────────────────────

def get_all_filaments(db_path: str) -> list:
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT f.*, lp.spool_cost AS live_cost, lp.on_sale, lp.price_live,
                   lp.error AS live_error, lp.updated_at AS price_updated,
                   fc.listed_qty, fc.ordered_qty
            FROM filaments f
            LEFT JOIN filament_live_prices lp ON f.sku = lp.sku
            LEFT JOIN filament_cart fc ON f.sku = fc.sku
            WHERE f.sku != 'misc'
            ORDER BY f.brand, f.colour
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_filament(db_path: str, sku: str) -> Optional[dict]:
    conn = get_connection(db_path)
    try:
        row = conn.execute("""
            SELECT f.*, lp.spool_cost AS live_cost, lp.on_sale, lp.price_live,
                   lp.error AS live_error, fc.listed_qty, fc.ordered_qty
            FROM filaments f
            LEFT JOIN filament_live_prices lp ON f.sku = lp.sku
            LEFT JOIN filament_cart fc ON f.sku = fc.sku
            WHERE f.sku=?
        """, (sku,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def upsert_filament(db_path: str, sku: str, **fields) -> None:
    conn = get_connection(db_path)
    now = datetime.now().isoformat()
    try:
        existing = conn.execute("SELECT sku FROM filaments WHERE sku=?", (sku,)).fetchone()
        if existing:
            sets = ", ".join(f"{k}=?" for k in fields)
            conn.execute(f"UPDATE filaments SET {sets}, updated_at=? WHERE sku=?",
                         [*fields.values(), now, sku])
        else:
            fields.update({"sku": sku, "created_at": now, "updated_at": now})
            cols = ", ".join(fields)
            ph = ", ".join("?" * len(fields))
            conn.execute(f"INSERT INTO filaments ({cols}) VALUES ({ph})", list(fields.values()))
        conn.commit()
    finally:
        conn.close()


def update_filament_counts(db_path: str, sku: str,
                           full_delta=0, partial_delta=0,
                           full_set=None, partial_set=None) -> tuple:
    conn = get_connection(db_path)
    try:
        row = conn.execute("SELECT full_spools, partial_spools FROM filaments WHERE sku=?", (sku,)).fetchone()
        if not row:
            return False, "Filament not found."
        nf = full_set if full_set is not None else max(0, row["full_spools"] + full_delta)
        np_ = partial_set if partial_set is not None else max(0, row["partial_spools"] + partial_delta)
        conn.execute("UPDATE filaments SET full_spools=?, partial_spools=?, updated_at=? WHERE sku=?",
                     (nf, np_, datetime.now().isoformat(), sku))
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def delete_filament_row(db_path: str, sku: str) -> tuple:
    conn = get_connection(db_path)
    try:
        r = conn.execute("DELETE FROM filaments WHERE sku=?", (sku,))
        conn.commit()
        return (True, None) if r.rowcount else (False, "Not found.")
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def upsert_live_price(db_path: str, sku: str, spool_cost, on_sale, price_live, error):
    conn = get_connection(db_path)
    try:
        conn.execute("""
            INSERT INTO filament_live_prices(sku,spool_cost,on_sale,price_live,error,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(sku) DO UPDATE SET
              spool_cost=excluded.spool_cost, on_sale=excluded.on_sale,
              price_live=excluded.price_live, error=excluded.error, updated_at=excluded.updated_at
        """, (sku, spool_cost, int(on_sale), int(price_live), error, datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Cart
# ─────────────────────────────────────────────────────────────────────────────

def db_add_to_cart(db_path: str, sku: str, qty: int) -> tuple:
    conn = get_connection(db_path)
    try:
        if not conn.execute("SELECT sku FROM filaments WHERE sku=?", (sku,)).fetchone():
            return False, "Filament not found."
        conn.execute("""
            INSERT INTO filament_cart(sku,listed_qty,ordered_qty,updated_at) VALUES(?,?,0,?)
            ON CONFLICT(sku) DO UPDATE SET listed_qty=listed_qty+excluded.listed_qty, updated_at=excluded.updated_at
        """, (sku, qty, datetime.now().isoformat()))
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def db_mark_ordered(db_path: str, sku: str, qty: int) -> tuple:
    conn = get_connection(db_path)
    try:
        conn.execute("""UPDATE filament_cart
            SET listed_qty=MAX(0,listed_qty-?), ordered_qty=ordered_qty+?, updated_at=? WHERE sku=?""",
            (qty, qty, datetime.now().isoformat(), sku))
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def db_mark_arrived(db_path: str, sku: str, qty: int) -> tuple:
    conn = get_connection(db_path)
    try:
        conn.execute("""UPDATE filament_cart
            SET ordered_qty=MAX(0,ordered_qty-?), updated_at=? WHERE sku=?""",
            (qty, datetime.now().isoformat(), sku))
        conn.execute("""UPDATE filaments SET full_spools=full_spools+?, updated_at=? WHERE sku=?""",
            (qty, datetime.now().isoformat(), sku))
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def get_cart(db_path: str) -> list:
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT fc.sku, fc.listed_qty, fc.ordered_qty,
                   f.brand, f.colour, f.material, f.image_url, f.url
            FROM filament_cart fc JOIN filaments f ON fc.sku=f.sku
            WHERE fc.listed_qty>0 OR fc.ordered_qty>0
            ORDER BY f.brand, f.colour
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Print jobs log
# ─────────────────────────────────────────────────────────────────────────────

def log_print_job(db_path: str, sku, item_name, variant_name, price, deal_text, template, qty):
    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO print_jobs(sku,item_name,variant_name,price,deal_text,template,qty) VALUES(?,?,?,?,?,?,?)",
            (sku, item_name, variant_name, price, deal_text, template, qty))
        conn.commit()
    finally:
        conn.close()


def get_print_jobs(db_path: str, limit=100) -> list:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM print_jobs ORDER BY printed_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Inventory / Stocktaking
# ─────────────────────────────────────────────────────────────────────────────

def upsert_inventory_item(db_path: str, item_id: str, name: str, sku: str = "", image_url: str = None):
    conn = get_connection(db_path)
    now = datetime.now().isoformat()
    try:
        conn.execute("""
            INSERT INTO inventory_items(id,name,sku,image_url,updated_at) VALUES(?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET name=excluded.name, sku=excluded.sku,
              image_url=excluded.image_url, updated_at=excluded.updated_at
        """, (item_id, name, sku, image_url, now))
        conn.commit()
    finally:
        conn.close()


def upsert_inventory_variant(db_path: str, variant_id: str, item_id: str, name: str, sku: str,
                              shopify_qty=None, square_qty=None,
                              shopify_price=None, square_price=None, image_url=None):
    conn = get_connection(db_path)
    now = datetime.now().isoformat()
    try:
        existing = conn.execute("SELECT id FROM inventory_variants WHERE id=?", (variant_id,)).fetchone()
        if existing:
            updates = []
            vals = []
            for col, val in [("name", name), ("sku", sku), ("item_id", item_id),
                              ("shopify_qty", shopify_qty), ("square_qty", square_qty),
                              ("shopify_price", shopify_price), ("square_price", square_price),
                              ("image_url", image_url)]:
                if val is not None:
                    updates.append(f"{col}=?")
                    vals.append(val)
            updates.append("updated_at=?")
            vals.append(now)
            vals.append(variant_id)
            conn.execute(f"UPDATE inventory_variants SET {', '.join(updates)} WHERE id=?", vals)
        else:
            conn.execute("""
                INSERT INTO inventory_variants
                  (id,item_id,name,sku,shopify_qty,square_qty,shopify_price,square_price,image_url,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (variant_id, item_id, name, sku, shopify_qty, square_qty,
                  shopify_price, square_price, image_url, now))
        conn.commit()
    finally:
        conn.close()


def get_all_inventory(db_path: str) -> list:
    conn = get_connection(db_path)
    try:
        items = conn.execute(
            "SELECT * FROM inventory_items ORDER BY name").fetchall()
        result = []
        for item in items:
            row = dict(item)
            variants = conn.execute(
                "SELECT * FROM inventory_variants WHERE item_id=? ORDER BY name",
                (item["id"],)).fetchall()
            row["variants"] = [dict(v) for v in variants]
            result.append(row)
        return result
    finally:
        conn.close()


def get_inventory_item(db_path: str, item_id: str) -> Optional[dict]:
    conn = get_connection(db_path)
    try:
        item = conn.execute("SELECT * FROM inventory_items WHERE id=?", (item_id,)).fetchone()
        if not item:
            return None
        row = dict(item)
        variants = conn.execute(
            "SELECT * FROM inventory_variants WHERE item_id=? ORDER BY name", (item_id,)).fetchall()
        row["variants"] = [dict(v) for v in variants]
        return row
    finally:
        conn.close()


def update_physical_count(db_path: str, variant_id: str, delta: int) -> tuple:
    conn = get_connection(db_path)
    now = datetime.now().isoformat()
    try:
        r = conn.execute(
            "UPDATE inventory_variants SET physical_count=MAX(0,physical_count+?), last_scanned=?, updated_at=? WHERE id=?",
            (delta, now, now, variant_id))
        conn.commit()
        return (True, None) if r.rowcount else (False, "Variant not found.")
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def update_physical_count_by_sku(db_path: str, sku: str, delta: int) -> tuple:
    conn = get_connection(db_path)
    now = datetime.now().isoformat()
    try:
        r = conn.execute(
            "UPDATE inventory_variants SET physical_count=MAX(0,physical_count+?), last_scanned=?, updated_at=? WHERE sku=?",
            (delta, now, now, sku))
        conn.commit()
        if r.rowcount:
            conn.execute("INSERT INTO scan_history(sku,change_amount) VALUES(?,?)", (sku, delta))
            conn.commit()
            return True, None
        return False, "SKU not found in inventory."
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def reset_all_physical_counts(db_path: str):
    conn = get_connection(db_path)
    try:
        conn.execute("UPDATE inventory_variants SET physical_count=0, last_scanned=NULL")
        conn.execute("DELETE FROM scan_history")
        conn.commit()
    finally:
        conn.close()


def get_scan_history(db_path: str, limit=200) -> list:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM scan_history ORDER BY scanned_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def undo_scan(db_path: str, scan_id: int) -> tuple:
    conn = get_connection(db_path)
    try:
        row = conn.execute("SELECT * FROM scan_history WHERE id=?", (scan_id,)).fetchone()
        if not row:
            return False, "Scan record not found."
        conn.execute(
            "UPDATE inventory_variants SET physical_count=MAX(0,physical_count-?), updated_at=? WHERE sku=?",
            (row["change_amount"], datetime.now().isoformat(), row["sku"]))
        conn.execute("DELETE FROM scan_history WHERE id=?", (scan_id,))
        conn.commit()
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def cleanup_inventory(db_path: str) -> int:
    """Remove variants/items with zero counts everywhere."""
    conn = get_connection(db_path)
    try:
        r = conn.execute("""DELETE FROM inventory_variants
            WHERE (shopify_qty IS NULL OR shopify_qty=0)
              AND (square_qty IS NULL OR square_qty=0)
              AND physical_count=0""")
        # Remove orphan items
        conn.execute("""DELETE FROM inventory_items WHERE id NOT IN
            (SELECT DISTINCT item_id FROM inventory_variants)""")
        conn.commit()
        return r.rowcount
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Print cost info (print_info.json → SQLite)
# ─────────────────────────────────────────────────────────────────────────────

def upsert_print_info_product(db_path: str, name: str, print_hours: int, print_minutes: int,
                              variants: list):
    """
    variants = [{"variant_name": str, "sku": str, "filaments": [{"id": str, "grams": float}]}]
    """
    conn = get_connection(db_path)
    now = datetime.now().isoformat()
    try:
        conn.execute("""
            INSERT INTO print_info_products(name,print_hours,print_minutes,updated_at) VALUES(?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET print_hours=excluded.print_hours,
              print_minutes=excluded.print_minutes, updated_at=excluded.updated_at
        """, (name, print_hours, print_minutes, now))
        for v in variants:
            conn.execute("""
                INSERT INTO print_info_variants(product_name,variant_name,sku,updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(product_name,sku) DO UPDATE SET variant_name=excluded.variant_name,
                  updated_at=excluded.updated_at
            """, (name, v["variant_name"], v["sku"], now))
            vid_row = conn.execute(
                "SELECT id FROM print_info_variants WHERE product_name=? AND sku=?",
                (name, v["sku"])).fetchone()
            if vid_row:
                vid = vid_row["id"]
                conn.execute("DELETE FROM print_info_filaments WHERE variant_id=?", (vid,))
                for f in v.get("filaments", []):
                    conn.execute(
                        "INSERT INTO print_info_filaments(variant_id,filament_id,grams) VALUES(?,?,?)",
                        (vid, f["id"], f["grams"]))
        conn.commit()
    finally:
        conn.close()


def get_print_info_for_sku(db_path: str, sku: str) -> Optional[dict]:
    conn = get_connection(db_path)
    try:
        row = conn.execute("""
            SELECT pv.id, pv.variant_name, pv.sku, pp.name as product_name,
                   pp.print_hours, pp.print_minutes
            FROM print_info_variants pv
            JOIN print_info_products pp ON pv.product_name=pp.name
            WHERE pv.sku=?
        """, (sku,)).fetchone()
        if not row:
            return None
        filaments = conn.execute(
            "SELECT filament_id, grams FROM print_info_filaments WHERE variant_id=?",
            (row["id"],)).fetchall()
        return {
            "print_hours": row["print_hours"],
            "print_minutes": row["print_minutes"],
            "filaments": [{"id": f["filament_id"], "grams": f["grams"]} for f in filaments],
        }
    finally:
        conn.close()


def get_all_print_info(db_path: str) -> list:
    conn = get_connection(db_path)
    try:
        products = conn.execute("SELECT * FROM print_info_products ORDER BY name").fetchall()
        result = []
        for p in products:
            variants_rows = conn.execute(
                "SELECT * FROM print_info_variants WHERE product_name=?", (p["name"],)).fetchall()
            variants = []
            for v in variants_rows:
                filaments = conn.execute(
                    "SELECT filament_id, grams FROM print_info_filaments WHERE variant_id=?",
                    (v["id"],)).fetchall()
                variants.append({
                    "variant_name": v["variant_name"],
                    "sku": v["sku"],
                    "filaments": [{"id": f["filament_id"], "grams": f["grams"]} for f in filaments],
                })
            result.append({
                "name": p["name"],
                "print_hours": p["print_hours"],
                "print_minutes": p["print_minutes"],
                "variants": variants,
            })
        return result
    finally:
        conn.close()


def get_inventory_discrepancies(db_path: str) -> dict:
    """Return items/variants with count or price discrepancies between platforms."""
    conn = get_connection(db_path)
    try:
        variants = conn.execute("""
            SELECT iv.*, ii.name as item_name
            FROM inventory_variants iv
            JOIN inventory_items ii ON iv.item_id=ii.id
        """).fetchall()

        count_discrepancies = []
        price_discrepancies = []
        missing_shopify = []
        missing_square = []

        for v in variants:
            sq = v["square_qty"]
            sh = v["shopify_qty"]
            sp = v["square_price"]
            shp = v["shopify_price"]

            # Count discrepancy: both exist but differ
            if sq is not None and sh is not None and sq != sh:
                count_discrepancies.append({
                    "item_name": v["item_name"], "variant_name": v["name"], "sku": v["sku"],
                    "square_qty": sq, "shopify_qty": sh, "physical_count": v["physical_count"],
                })

            # Price discrepancy
            if sp is not None and shp is not None and abs(sp - shp) > 0.01:
                price_discrepancies.append({
                    "item_name": v["item_name"], "variant_name": v["name"], "sku": v["sku"],
                    "square_price": sp, "shopify_price": shp,
                })

            # Missing from a platform
            if sh is None or sh == 0:
                if sq is not None and sq > 0:
                    missing_shopify.append({
                        "item_name": v["item_name"], "variant_name": v["name"], "sku": v["sku"],
                        "square_qty": sq,
                    })
            if sq is None or sq == 0:
                if sh is not None and sh > 0:
                    missing_square.append({
                        "item_name": v["item_name"], "variant_name": v["name"], "sku": v["sku"],
                        "shopify_qty": sh,
                    })

        return {
            "count_discrepancies": count_discrepancies,
            "price_discrepancies": price_discrepancies,
            "missing_shopify": missing_shopify,
            "missing_square": missing_square,
        }
    finally:
        conn.close()
