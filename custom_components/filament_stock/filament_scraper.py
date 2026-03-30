"""filament_scraper.py — Live price scraping (Bambu/Siddament via curl_cffi, Amazon via Selenium)."""
import re, time, random, subprocess, logging
from typing import Optional

_LOGGER = logging.getLogger(__name__)

try:
    from curl_cffi import requests as cffi_requests
    from lxml import etree
    _SCRAPE_OK = True
except ImportError:
    _SCRAPE_OK = False
    _LOGGER.warning("curl_cffi/lxml not installed — live scraping disabled")

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import NoSuchElementException
    from webdriver_manager.chrome import ChromeDriverManager
    _SELENIUM_OK = True
except ImportError:
    _SELENIUM_OK = False

_DRIVER = None


def _init_driver():
    global _DRIVER
    if _DRIVER:
        try:
            _DRIVER.title
            return _DRIVER
        except Exception:
            _DRIVER = None
    if not _SELENIUM_OK:
        return None
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    try:
        ver = subprocess.check_output(
            ["google-chrome", "--version"], stderr=subprocess.DEVNULL
        ).decode().strip().split()[-1]
        svc = Service(ChromeDriverManager(driver_version=ver).install())
    except Exception:
        svc = Service(ChromeDriverManager().install())
    _DRIVER = webdriver.Chrome(service=svc, options=opts)
    _DRIVER.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
    return _DRIVER


def scrape_price(url: str, driver=None) -> tuple:
    """Returns (price_aud, on_sale, error_or_none)."""
    if not url:
        return None, False, "No URL"
    if not _SCRAPE_OK:
        return None, False, "Scraping deps not installed"

    if "bambulab" in url or "siddament" in url:
        try:
            sess = cffi_requests.Session()
            html = sess.get(url, impersonate="chrome").text
            tree = etree.HTML(html)
            px = ("//main//span[contains(normalize-space(.),'AUD') and "
                  "not(contains(@class,'line-through'))]/ancestor::div[1]")
            parents = tree.xpath(px)
            if not parents:
                return None, False, "No price container"
            spans = parents[0].xpath(
                ".//span[contains(normalize-space(.),'AUD') and not(contains(@class,'line-through'))]")
            on_sale = len(spans) > 1
            price = float((spans[0].text or "").replace("AUD","").replace("$","").strip())
            return price, on_sale, None
        except Exception as e:
            return None, False, str(e)

    elif "amazon" in url:
        drv = driver or _init_driver()
        if not drv:
            return None, False, "Selenium unavailable"
        try:
            drv.get(url)
            time.sleep(2.5)
            for xp in ["//span[@id='priceblock_ourprice']", "//span[@id='priceblock_dealprice']",
                       "//span[@id='price_inside_buybox']", "//span[contains(@class,'a-price-whole')][1]"]:
                try:
                    t = drv.find_element(By.XPATH, xp).text
                    if t:
                        price = float(t.replace("AUD","").replace("$","").replace(",","").strip())
                        discount = False
                        try:
                            bb = drv.find_element(By.ID, "desktop_buybox").text.lower()
                            discount = bool(re.search(r"\d+(\.\d+)?%", bb)) or "save" in bb
                        except Exception:
                            pass
                        return price, discount, None
                except NoSuchElementException:
                    continue
            return None, False, "Amazon price element not found"
        except Exception as e:
            return None, False, str(e)
    else:
        return None, False, "Unsupported site"


def refresh_all_prices(db_path: str):
    from .database import get_all_filaments, upsert_live_price, set_last_price_update
    _LOGGER.info("Starting filament price refresh")
    driver = _init_driver() if _SELENIUM_OK else None

    for f in get_all_filaments(db_path):
        sku = f.get("sku")
        url = f.get("url")
        if not url:
            continue
        price, on_sale, error = scrape_price(url, driver)
        time.sleep(random.uniform(1, 2))
        if price is not None:
            upsert_live_price(db_path, sku, price, on_sale, True, None)
        else:
            fallback = f.get("spool_cost")
            if fallback is not None:
                upsert_live_price(db_path, sku, fallback, False, False, error)

    set_last_price_update(db_path)
    _LOGGER.info("Filament price refresh complete")
