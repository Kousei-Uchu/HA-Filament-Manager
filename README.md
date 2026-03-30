# Filament Stock Manager — Home Assistant Integration

A full-featured HACS integration porting your **stock_api** FastAPI backend and **Swift app** into Home Assistant.

---

## Features

| Area | What's implemented |
|------|-------------------|
| **Filaments** | Full CRUD, spool lifecycle (open/finish/receive), live price scraping, on-sale detection |
| **Shopping Cart** | Add to order queue, mark ordered, mark arrived (auto-adds stock) |
| **Stocktaking** | Full Shopify + Square inventory sync, physical count scanning, undo scan, push counts to Square |
| **Discrepancies** | Count mismatches, price mismatches, missing from Shopify, missing from Square |
| **Reports** | Profit/COGS report from Square sales data over a date range |
| **Print / Add Stock** | Full label printing pipeline via `brother_ql_print.print_label` |
| **History** | Scan history with undo, print job log |
| **Entities** | One HA device per filament type, 3 entities each (spools, price, cart) + 10 summary sensors |
| **SQLite** | All data stored in `/config/filament_stock.db` — no JSON files |

---

## Architecture

```
custom_components/
├── filament_stock/
│   ├── __init__.py          ← REST API views, HA services, scheduler, panel
│   ├── database.py          ← SQLite schema + all CRUD operations
│   ├── filaments.py         ← Filament business logic
│   ├── filament_scraper.py  ← Live price scraping (Bambu, Siddament, Amazon)
│   ├── platform_sync.py     ← Shopify + Square API clients + inventory sync
│   ├── cost_calculator.py   ← COGS calculation (electricity + filament cost)
│   ├── sensor.py            ← HA entities: 1 device per filament + summary sensors
│   ├── config_flow.py       ← Setup wizard
│   ├── manifest.json
│   ├── strings.json
│   └── www/
│       └── panel.html       ← Full dashboard UI
│
└── brother_ql_print/
    ├── __init__.py          ← print_label service (fixed + model-aware)
    ├── printing.py          ← LBX renderer + brother_ql driver (bug-fixed)
    ├── config_flow.py       ← Full label/model selection with options flow
    ├── manifest.json
    ├── services.yaml
    └── strings.json

migration_scripts/
├── migrate_to_sqlite.py     ← All JSON files → SQLite (idempotent)
└── export_to_json.py        ← SQLite → JSON (rollback)
```

---

## Installation

### 1. Prerequisites

```bash
# On your HA host (or in a venv for migration):
pip install brother_ql Pillow "qrcode[pil]" lxml curl_cffi selenium webdriver-manager square requests
```

### 2. Copy integration files

```bash
cp -r custom_components/filament_stock   /config/custom_components/
cp -r custom_components/brother_ql_print /config/custom_components/
```

Or add this repository to HACS as a custom integration.

### 3. Place your .lbx label templates

```
/config/labels/tag_label.lbx
/config/labels/mini_label.lbx
/config/labels/egg_label.lbx
/config/labels/mini_priceless_label.lbx
```

### 4. Migrate existing data

```bash
python migration_scripts/migrate_to_sqlite.py \
  --filaments  shared_data_storage/filaments/filaments.json \
  --live        shared_data_storage/filaments/live_filament_info.json \
  --cart        stock_api/api_data_storage/cart.json \
  --print-info  shared_data_storage/prints/print_info.json \
  --output      /config/filament_stock.db
```

### 5. Set up the integrations

Restart HA, then go to **Settings → Devices & Services → Add Integration**:

1. **Brother QL Printer** → select model (e.g. QL-700) and tape size (e.g. 62mm)
2. **Filament Stock Manager** → enter .lbx paths and API credentials

---

## Entity structure

Every filament type gets its own **HA device** with three entities:

| Entity | Unit | Description |
|--------|------|-------------|
| `sensor.filament_stock_<sku>_spools` | spools | Total spools (full+partial). Attributes: all metadata. |
| `sensor.filament_stock_<sku>_price`  | AUD   | Current spool cost. Attributes: live/default/sale status. |
| `sensor.filament_stock_<sku>_cart`   | spools | Cart queue (listed + ordered). |

Global summary sensors:

| Entity | Description |
|--------|-------------|
| `sensor.filament_total_spools` | All spools across all filaments |
| `sensor.filament_types` | Number of distinct filament types |
| `sensor.filament_low_stock_count` | Types with zero full spools |
| `sensor.filament_cart_items` | Items in to-order queue |
| `sensor.filament_last_price_update` | Timestamp of last scrape |
| `sensor.filaments_on_sale` | Count currently on sale |
| `sensor.inventory_product_count` | Shopify/Square product count |
| `sensor.inventory_discrepancies` | Total stock/price issues |
| `sensor.last_inventory_sync` | Timestamp of last platform sync |
| `sensor.stocktake_scan_count` | Number of scans in current session |

---

## REST API

All endpoints require HA Bearer auth. The panel uses these automatically.

### Filaments
| Method | Path | Action |
|--------|------|--------|
| GET  | `/api/filament_stock/filaments/list` | List all filaments |
| POST | `/api/filament_stock/filaments/new` | Add stock / create filament |
| POST | `/api/filament_stock/filaments/set` | Set absolute counts |
| POST | `/api/filament_stock/filaments/open/{id}` | Open a full spool |
| POST | `/api/filament_stock/filaments/finish/{id}` | Finish a partial spool |
| POST | `/api/filament_stock/filaments/modify/{id}` | Update metadata |
| POST | `/api/filament_stock/filaments/delete/{id}` | Delete filament |
| GET  | `/api/filament_stock/filaments/cart` | List cart |
| POST | `/api/filament_stock/filaments/cart/add` | Add to cart |
| POST | `/api/filament_stock/filaments/cart/ordered` | Mark as ordered |
| POST | `/api/filament_stock/filaments/cart/arrived` | Mark arrived → adds stock |

### Inventory / Stocktaking
| Method | Path | Action |
|--------|------|--------|
| GET  | `/api/filament_stock/inventory/list` | All products + variants |
| POST | `/api/filament_stock/inventory/sync` | Sync from Shopify + Square |
| POST | `/api/filament_stock/inventory/scan` | Record a physical count scan |
| POST | `/api/filament_stock/inventory/undo/{id}` | Undo a scan |
| POST | `/api/filament_stock/inventory/reset` | Reset all counts |
| POST | `/api/filament_stock/inventory/push_square` | Push counts to Square |
| GET  | `/api/filament_stock/inventory/discrepancies` | Get all discrepancies |
| POST | `/api/filament_stock/inventory/cleanup` | Remove zero-count items |

### Reports & COGS
| Method | Path | Action |
|--------|------|--------|
| GET  | `/api/filament_stock/reports/profit?start_date=&end_date=` | Profit report |
| GET  | `/api/filament_stock/cogs?sku=` | COGS for one SKU |

### Print info
| Method | Path | Action |
|--------|------|--------|
| GET  | `/api/filament_stock/print_info/list` | All print info products |
| POST | `/api/filament_stock/print_info/upsert` | Add/update a product |

### History
| Method | Path | Action |
|--------|------|--------|
| GET  | `/api/filament_stock/history/scans?limit=200` | Scan history |
| GET  | `/api/filament_stock/history/prints?limit=100` | Print job history |

---

## HA Services

```yaml
# Open a full spool (full → partial)
service: filament_stock.open_spool
data:
  filament_id: bambu_lab-pla-jade_white

# Finish a partial spool
service: filament_stock.finish_spool
data:
  filament_id: bambu_lab-pla-jade_white

# Receive new spools
service: filament_stock.new_spool
data:
  filament_id: bambu_lab-pla-jade_white
  full_rolls: 2

# Trigger live price refresh
service: filament_stock.refresh_prices

# Sync inventory from Shopify + Square
service: filament_stock.sync_inventory

# Record a barcode scan (+1 to physical count)
service: filament_stock.scan_item
data:
  sku: "Dragon-Baby-Crystal-CrushedBlue"
  change_amount: 1

# Reset all physical counts + scan history
service: filament_stock.reset_counts

# Push physical counts to Square
service: filament_stock.push_to_square
```

---

## Printing

The `brother_ql_print.print_label` service is called automatically by the **Print / Add Stock** tab, but you can also use it in automations:

```yaml
service: brother_ql_print.print_label
data:
  template: /config/labels/tag_label.lbx
  quantity: 5
  fields:
    Title: "Blue Dragon XL"
    Price: "$12.00"
    Colour: "Galaxy Blue"
    Deal Tag: "or 2 for $10.00"
    Variant Barcode: "https://aidens3dp.com/30322695"
```

### Label → field name mapping

| Template | Fields |
|----------|--------|
| `tag_label.lbx` | `Title`, `Variant Barcode`, `Price`, `Colour`, `Deal Tag` |
| `mini_label.lbx` (4-up sticker) | `Variant Barcode 1–4`, `Tag $ 1–4` |
| `egg_label.lbx` | `Title`, `Deal Tag`, `Colour`, `Price`, `Variant Barcode` |
| `mini_priceless_label.lbx` (4-up) | `Variant Barcode 1–4` |

Quantity is automatically divided by 4 for the 4-up templates (preserving the original `print_barcodes` behaviour).

---

## Bug fixes in this version vs original

| Component | Fix |
|-----------|-----|
| `brother_ql_print/printing.py` | Removed debug `draw.rectangle(outline="red")` calls |
| `brother_ql_print/printing.py` | `get_printer_and_label` now accepts `model_override` from config |
| `brother_ql_print/printing.py` | `print_image` and `render_and_print` signatures unified |
| `brother_ql_print/__init__.py` | Model and label size from config entry passed through to printer |
| `filament_stock/filaments.py` | `delete_spool` fixed (`del data["filaments"][i]` → correct list deletion) |
| `filament_stock/filaments.py` | `set_spool` bug: `full_delta` used instead of `full_set` — fixed |
| `filament_stock/database.py` | `mark_arrived` now also increments `full_spools` (was missing) |
| `filament_stock/filament_scraper.py` | Selenium driver checks if still alive before reusing |
| `filament_stock/cost_calculator.py` | `get_filament_cost_per_gram` now uses live price when available |

---

## Rollback

```bash
python migration_scripts/export_to_json.py \
  --db      /config/filament_stock.db \
  --out-dir /path/to/shared_data_storage/
```

Produces `filaments.json`, `live_filament_info.json`, `cart.json`, `print_info.json` in original format.
