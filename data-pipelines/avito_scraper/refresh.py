# -*- coding: utf-8 -*-
"""
refresh.py
Scrapes Avito, diffs against Supabase by LINK, and:
  1. Inserts genuinely new items into the `new_laptops` staging table
  2. PATCHes price changes directly in `laptops`
  3. Marks items not found in the scrape as is_sold=True
  4. Saves a stats row to `pipeline_stats`

Usage:
    python refresh.py              # scrape 500 pages (default)
    python refresh.py -p 5         # scrape 5 pages (quick test)
"""

import asyncio
import logging
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from scraper import scrape

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")


def _headers(content_type=True):
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if content_type:
        h["Content-Type"] = "application/json"
    return h


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------
def fetch_db_items():
    """Fetch link, avito_id, price, is_sold from every row in laptops."""
    logger.info("Fetching current database items...")
    all_rows = []
    offset = 0
    step = 1000

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/laptops"
            f"?select=link,avito_id,price,is_sold"
            f"&order=id.asc"
            f"&offset={offset}&limit={step}"
        )
        resp = requests.get(url, headers=_headers(content_type=False))
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_rows.extend(data)
        offset += step
        if len(data) < step:
            break

    logger.info("Fetched %d items from database.", len(all_rows))
    return all_rows


def insert_into_new_laptops(items: list[dict]):
    """Batch-insert items into the new_laptops staging table."""
    if not items:
        return
    batch_size = 200
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        payload = []
        for item in batch:
            payload.append({
                "avito_id": str(item.get("avito_id", "")),
                "description": str(item.get("description", "")),
                "price": float(item.get("price", 0) or 0),
                "city": str(item.get("city", "")),
                "link": str(item.get("link", "")),
                "is_shop": bool(item.get("is_shop", False)),
                "has_delivery": bool(item.get("has_delivery", False)),
            })
        url = f"{SUPABASE_URL}/rest/v1/new_laptops"
        resp = requests.post(url, headers=_headers(), json=payload)
        if not resp.ok:
            logger.error("Insert new_laptops failed: %d %s", resp.status_code, resp.text[:300])
        else:
            logger.info("Inserted batch %d-%d into new_laptops", i + 1, min(i + batch_size, len(items)))
        time.sleep(0.1)


def patch_prices(updates: list[dict]):
    """PATCH price for items with same link but different price."""
    if not updates:
        return
    logger.info("Updating prices for %d items...", len(updates))
    for item in updates:
        link = item["link"]
        new_price = float(item["price"])
        url = f"{SUPABASE_URL}/rest/v1/laptops?link=eq.{requests.utils.quote(link, safe='')}"
        resp = requests.patch(url, headers=_headers(), json={"price": new_price, "is_sold": False})
        if not resp.ok:
            logger.error("Price update failed for %s: %d %s", link, resp.status_code, resp.text[:200])
        time.sleep(0.05)
    logger.info("Price updates complete.")


def mark_sold(links_not_found: set[str], db_items: list[dict]):
    """Set is_sold=True for DB items whose link was NOT in the scrape."""
    if not links_not_found:
        return
    logger.info("Marking %d items as sold...", len(links_not_found))
    sold_ids = [str(row["avito_id"]) for row in db_items if row.get("link") in links_not_found]
    batch_size = 100
    for i in range(0, len(sold_ids), batch_size):
        batch = sold_ids[i:i + batch_size]
        ids_param = ",".join(batch)
        url = f"{SUPABASE_URL}/rest/v1/laptops?avito_id=in.({ids_param})"
        resp = requests.patch(url, headers=_headers(), json={"is_sold": True})
        if not resp.ok:
            logger.error("Mark sold failed: %d %s", resp.status_code, resp.text[:200])
        time.sleep(0.1)
    logger.info("Marked %d items as sold.", len(sold_ids))

def unsell_active(links_found: set[str], db_items: list[dict]):
    """Set is_sold=False for DB items that ARE in the scrape (un-sell relisted items)."""
    # Only un-sell items that were previously sold
    relisted_ids = [
        str(row["avito_id"]) for row in db_items
        if row.get("link") in links_found and row.get("is_sold") is True
    ]
    if not relisted_ids:
        return
    # Batched update
    batch_size = 500
    for i in range(0, len(relisted_ids), batch_size):
        batch = relisted_ids[i:i + batch_size]
        ids_param = ",".join(batch)
        url = f"{SUPABASE_URL}/rest/v1/laptops?avito_id=in.({ids_param})"
        resp = requests.patch(url, headers=_headers(), json={"is_sold": False})
        if not resp.ok:
            logger.error("Un-sell failed: %d %s", resp.status_code, resp.text[:200])
        time.sleep(0.1)
    logger.info("Confirmed/Reverted %d items to active (is_sold=False).", len(relisted_ids))




def save_pipeline_stats(new_count: int):
    """Insert a stats row into pipeline_stats."""
    url = f"{SUPABASE_URL}/rest/v1/pipeline_stats"
    payload = {"new_items": new_count}
    resp = requests.post(url, headers=_headers(), json=payload)
    if not resp.ok:
        logger.error("Failed to save stats: %d %s", resp.status_code, resp.text[:200])
    else:
        logger.info("Saved pipeline stats: %d new items.", new_count)


# ---------------------------------------------------------------------------
# Diff logic
# ---------------------------------------------------------------------------
def diff_and_act(scraped_ads: list[dict], db_items: list[dict]):
    """
    Compare scraped ads against DB by LINK.

    Categories:
      1. New link + new avito_id  -> insert into new_laptops
      2. Same link + new price    -> PATCH price in laptops
      3. New link + old avito_id  -> insert into new_laptops (overwrite via upsert)
      4. DB link not in scrape    -> mark is_sold=True
      5. Same link + same price   -> skip
    """
    # Build DB indexes
    db_links = {}        # link -> {avito_id, price}
    db_avito_ids = set()
    for row in db_items:
        link = str(row.get("link", "")).strip()
        aid = str(row.get("avito_id", "")).strip()
        price = float(row.get("price", 0) or 0)
        if link:
            db_links[link] = {"avito_id": aid, "price": price}
        if aid:
            db_avito_ids.add(aid)

    # Categorize scraped items
    new_items = []          # Categories 1 + 3 -> go to new_laptops
    price_updates = []      # Category 2 -> PATCH directly
    scraped_link_set = set()

    stats = {"new": 0, "recycled": 0, "price_changed": 0, "unchanged": 0}

    for ad in scraped_ads:
        link = str(ad.get("link", "")).strip()
        aid = str(ad.get("avito_id", "")).strip()
        price = float(ad.get("price", 0) or 0)

        if not link:
            continue
        scraped_link_set.add(link)

        if link not in db_links:
            # New link
            if aid in db_avito_ids:
                # Category 3: recycled avito_id (new laptop, same seller ID)
                stats["recycled"] += 1
            else:
                # Category 1: genuinely new
                stats["new"] += 1
            new_items.append(ad)
        else:
            # Link exists in DB
            db_price = db_links[link]["price"]
            if abs(price - db_price) > 0.01:
                # Category 2: price changed
                stats["price_changed"] += 1
                price_updates.append({"link": link, "price": price})
            else:
                # Category 5: unchanged
                stats["unchanged"] += 1

    # Category 4: DB items not found in scrape
    db_link_set = set(db_links.keys())
    links_not_found = db_link_set - scraped_link_set
    links_found = db_link_set & scraped_link_set

    # Log summary
    logger.info("=" * 50)
    logger.info("DIFF SUMMARY")
    logger.info("=" * 50)
    logger.info("  New items (new link+id):     %d", stats["new"])
    logger.info("  Recycled (new link+old id):  %d", stats["recycled"])
    logger.info("  Price changed:               %d", stats["price_changed"])
    logger.info("  Unchanged (skipped):         %d", stats["unchanged"])
    logger.info("  Not found (sold?):           %d", len(links_not_found))
    logger.info("=" * 50)

    # Act on each category
    insert_into_new_laptops(new_items)
    patch_prices(price_updates)
    mark_sold(links_not_found, db_items)
    unsell_active(links_found, db_items)

    total_new = stats["new"] + stats["recycled"]
    save_pipeline_stats(total_new)

    return total_new


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(max_pages: int = 500):
    start = datetime.now()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL/SUPABASE_KEY not found in .env")
        return

    # 1. Fetch current DB state
    db_items = fetch_db_items()

    # 2. Scrape Avito
    scraped_ads = asyncio.run(scrape(max_pages))
    logger.info("Scraped %d unique ads from %d pages.", len(scraped_ads), max_pages)

    # 3. Diff and act
    new_count = diff_and_act(scraped_ads, db_items)

    # 4. Summary
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\nRefresh complete in {elapsed:.1f}s")
    print(f"  {new_count} items inserted into new_laptops for parsing")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Avito refresh pipeline")
    parser.add_argument("-p", "--pages", type=int, default=500,
                        help="Number of pages to scrape (default: 500)")
    args = parser.parse_args()
    main(args.pages)
