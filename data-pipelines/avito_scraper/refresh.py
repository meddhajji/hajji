# -*- coding: utf-8 -*-
"""
refresh.py
Scrapes Avito, diffs against Supabase, outputs:
  - new.csv   : items that are new OR have a changed price/laptop (need parsing)
  - old.csv   : items in DB but not found in scrape (likely sold)

Items that are literally identical (same id, same price, same description)
are silently skipped.

The "last scrape date" is derived from MAX(updated_at) in the database,
no dedicated storage needed.

Usage:
    python refresh.py              # scrape 500 pages (default)
    python refresh.py -p 50        # scrape 50 pages (quick test)
"""

import asyncio
import csv
import re
import logging
import os
import time
from pathlib import Path
from datetime import datetime

import requests
from dotenv import load_dotenv
from scraper import scrape, CSV_COLUMNS, compress_text

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OUTPUT_DIR = Path(__file__).parent / "data"

# ---------------------------------------------------------------------------
# Supabase helpers (REST API, no Python client needed)
# ---------------------------------------------------------------------------
def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

def fetch_db_items():
    """Fetch avito_id, price, description, link from Supabase."""
    logger.info("Fetching current database items...")
    all_rows = []
    offset = 0
    step = 1000

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/laptops"
            f"?select=avito_id,price,description,link"
            f"&order=avito_id.asc"
            f"&offset={offset}&limit={step}"
        )
        resp = requests.get(url, headers=_sb_headers())
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


def fetch_last_scrape_date():
    """Derive last scrape date from MAX(updated_at) in the database."""
    url = (
        f"{SUPABASE_URL}/rest/v1/laptops"
        f"?select=updated_at"
        f"&order=updated_at.desc"
        f"&limit=1"
    )
    resp = requests.get(url, headers=_sb_headers())
    resp.raise_for_status()
    data = resp.json()
    if data:
        return data[0]["updated_at"]
    return None


# ---------------------------------------------------------------------------
# Similarity check (detect recycled listings)
# ---------------------------------------------------------------------------
def extract_url_title(link: str) -> str:
    """Extract the title part from an Avito URL.
    URL format: .../ordinateurs_portables/Model_Name_Here_57173341.htm
    Returns: 'model name here' (lowercase, underscores replaced with spaces)
    """
    if not link:
        return ""
    try:
        # Get filename from URL path
        filename = link.rstrip("/").split("/")[-1]  # Model_Name_57173341.htm
        filename = filename.replace(".htm", "")       # Model_Name_57173341
        # Remove the trailing numeric ID (always the last _segment)
        parts = filename.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            title = parts[0]
        else:
            title = filename
        return title.lower().replace("_", " ").strip()
    except Exception:
        return ""


def jaccard_similarity(s1: str, s2: str) -> float:
    """Word-level Jaccard similarity between two strings."""
    if not s1 or not s2:
        return 0.0
    words1 = set(s1.lower().split())
    words2 = set(s2.lower().split())
    if not words1 or not words2:
        return 0.0
    intersection = words1 & words2
    union = words1 | words2
    return len(intersection) / len(union)


# ---------------------------------------------------------------------------
# Diff logic
# ---------------------------------------------------------------------------
def diff(scraped_ads: list[dict], db_items: list[dict]):
    """
    Compare scraped ads against database items.

    Returns:
        new_items:  list of dicts to write to new.csv
                    (brand new, price changed, or recycled listing)
        old_items:  list of dicts to write to old.csv
                    (in DB but not found in scrape)
    """
    # Index DB by avito_id
    db_index = {}
    for row in db_items:
        aid = str(row.get("avito_id", "")).strip()
        if aid:
            db_index[aid] = row

    scraped_ids = set()
    new_items = []

    for ad in scraped_ads:
        aid = str(ad.get("avito_id", "")).strip()
        if not aid:
            continue
        scraped_ids.add(aid)

        if aid not in db_index:
            # brand new item
            ad["_status"] = "new"
            new_items.append(ad)
            continue

        db_row = db_index[aid]

        # compare price
        scraped_price = float(ad.get("price", 0) or 0)
        db_price = float(db_row.get("price", 0) or 0)
        price_changed = abs(scraped_price - db_price) > 0.01

        # detect recycled listings using the short URL title (reliable)
        scraped_title = extract_url_title(str(ad.get("link", "")))
        db_title = extract_url_title(str(db_row.get("link", "")))
        title_sim = jaccard_similarity(scraped_title, db_title)
        is_recycled = title_sim < 0.3  # less than 30% word overlap = different laptop

        if is_recycled:
            ad["_status"] = "recycled"
            new_items.append(ad)
        elif price_changed:
            ad["_status"] = "price_changed"
            new_items.append(ad)
        # else: identical item, skip silently

    # Items in DB but not in scrape = potentially sold
    old_items = []
    for aid, row in db_index.items():
        if aid not in scraped_ids:
            old_items.append(row)

    return new_items, old_items


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------
NEW_CSV_COLUMNS = CSV_COLUMNS + ["_status"]

def save_new_csv(items: list[dict], path: Path):
    """Save new/changed items with a _status column."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=NEW_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for item in items:
            row = {col: item.get(col, "") for col in NEW_CSV_COLUMNS}
            writer.writerow(row)
    logger.info("Saved %d items to %s", len(items), path)


def save_old_csv(items: list[dict], path: Path):
    """Save sold/not-found items (only the columns we fetched from DB)."""
    cols = ["avito_id", "price", "description", "link"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for item in items:
            writer.writerow({c: item.get(c, "") for c in cols})
    logger.info("Saved %d items to %s", len(items), path)


def mark_items_as_sold(old_items: list[dict]):
    """Send PATCH request to Supabase to set is_sold=True for items not found."""
    if not old_items:
        return
    logger.info("Marking %d items as sold in Supabase...", len(old_items))
    batch_size = 100
    for i in range(0, len(old_items), batch_size):
        batch = old_items[i:i + batch_size]
        ids = [str(item.get("avito_id", "")) for item in batch if item.get("avito_id")]
        if not ids:
            continue
        ids_param = ",".join(ids)
        url = f"{SUPABASE_URL}/rest/v1/laptops?avito_id=in.({ids_param})"
        resp = requests.patch(url, headers=_sb_headers(), json={"is_sold": True})
        if not resp.ok:
            logger.error("Failed to mark sold items: %d %s", resp.status_code, resp.text[:200])
        time.sleep(0.1)


def save_pipeline_stats(new_count: int, updated_count: int, sold_count: int, total_count: int):
    """Insert daily refresh stats into pipeline_stats table."""
    logger.info("Saving pipeline stats to Supabase...")
    url = f"{SUPABASE_URL}/rest/v1/pipeline_stats"
    payload = {
        "new_items": new_count,
        "updated_items": updated_count,
        "sold_items": sold_count,
        "total_items": total_count
    }
    resp = requests.post(url, headers=_sb_headers(), json=payload)
    if not resp.ok:
        logger.error("Failed to save stats: %d %s", resp.status_code, resp.text[:200])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(max_pages: int = 500):
    start = datetime.now()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Fetch current DB state
    db_items = fetch_db_items()
    last_scrape = fetch_last_scrape_date()
    logger.info("Last scrape date (from max updated_at): %s", last_scrape)

    # 2. Scrape Avito
    scraped_ads = asyncio.run(scrape(max_pages))
    logger.info("Scraped %d unique ads from %d pages.", len(scraped_ads), max_pages)

    # 3. Diff
    new_items, old_items = diff(scraped_ads, db_items)

    # Count by status
    status_counts = {}
    for item in new_items:
        s = item.get("_status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

    # 4. Save files and update Supabase
    new_path = OUTPUT_DIR / "new.csv"
    old_path = OUTPUT_DIR / "old.csv"

    save_new_csv(new_items, new_path)
    save_old_csv(old_items, old_path)
    mark_items_as_sold(old_items)

    new_count = status_counts.get("new", 0)
    updated_count = status_counts.get("price_changed", 0) + status_counts.get("recycled", 0)
    sold_count = len(old_items)
    
    # We estimate total active items as previous total + new - sold
    # Alternatively, len(scraped_ads) is the active scraped inventory.
    total_count = len(scraped_ads)
    
    save_pipeline_stats(new_count, updated_count, sold_count, total_count)

    # 5. Summary
    elapsed = (datetime.now() - start).total_seconds()
    print("\n" + "=" * 50)
    print("REFRESH SUMMARY")
    print("=" * 50)
    print(f"Database items:        {len(db_items)}")
    print(f"Scraped items:         {len(scraped_ads)}")
    print(f"Last scrape date:      {last_scrape}")
    print()
    for status, count in sorted(status_counts.items()):
        print(f"  {status:18s}   {count}")
    print(f"  {'total changes':18s}   {len(new_items)}")
    print()
    print(f"Not found (sold?):     {len(old_items)}")
    print(f"Unchanged (skipped):   {len(scraped_ads) - len(new_items) - len(old_items)}")
    print()
    print(f"Saved: {new_path}")
    print(f"       {old_path}")
    print(f"Time:  {elapsed:.1f}s")
    print("=" * 50)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Avito refresh pipeline")
    parser.add_argument("-p", "--pages", type=int, default=500,
                        help="Number of pages to scrape (default: 500)")
    args = parser.parse_args()
    main(args.pages)
