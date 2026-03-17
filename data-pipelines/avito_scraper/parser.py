# -*- coding: utf-8 -*-
"""
parser.py
Reads items from the `new_laptops` staging table, parses specs with Gemini,
calculates scores, upserts into `laptops`, and deletes the processed rows
from `new_laptops`.

Loops until the staging table is empty.

Usage:
    python parser.py                 # process all items
    python parser.py -n 50           # process only the first 50 items (test)
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google import genai

from score_laptops import calc_laptop_score

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GEMINI_KEY = os.getenv("GEMINI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
MODEL = "gemini-3.1-flash-lite-preview"
GEMINI_BATCH = 200    # items per Gemini API call
MAX_RETRIES = 3      # max parse attempts per item

# Columns that Gemini must extract
SPEC_COLS = [
    "brand", "model", "cpu", "ram", "storage", "ssd",
    "gpu", "gpu_type", "gpu_vram",
    "screen_size", "refresh_rate", "new", "touchscreen",
]

NUMERIC_COLS = ["price", "ram", "storage", "ssd", "gpu_vram",
                "screen_size", "refresh_rate", "new", "touchscreen"]
BOOL_COLS = ["is_shop", "has_delivery"]

# 3 reference examples
EXAMPLES = """[
  {
    "brand": "Lenovo",
    "model": "Thinkbook Ultra 7",
    "cpu": "Core Ultra 7 155u",
    "ram": 32,
    "storage": 1000,
    "ssd": 1,
    "gpu": "Integrated",
    "gpu_type": "Integrated",
    "gpu_vram": null,
    "screen_size": 14,
    "refresh_rate": null,
    "new": null,
    "touchscreen": null
  },
  {
    "brand": "Apple",
    "model": "Macbook Pro 14",
    "cpu": "M4",
    "ram": 16,
    "storage": 512,
    "ssd": 1,
    "gpu": null,
    "gpu_type": null,
    "gpu_vram": null,
    "screen_size": 14,
    "refresh_rate": null,
    "new": 1,
    "touchscreen": null
  },
  {
    "brand": "Dell",
    "model": "Latitude 5400",
    "cpu": "i5-8350u",
    "ram": 8,
    "storage": 256,
    "ssd": 1,
    "gpu": null,
    "gpu_type": "Integrated",
    "gpu_vram": null,
    "screen_size": 14,
    "refresh_rate": null,
    "new": null,
    "touchscreen": null
  }
]"""

SYSTEM_PROMPT = f"""You are a laptop spec extractor. You receive a list of Avito laptop descriptions and extract structured specs from each one.

RULES:
- Return a JSON array with one object per description, in the same order.
- Each object must have exactly these keys: {json.dumps(SPEC_COLS)}
- "brand" and "model": capitalize only the first letter of each word. Never write consecutive capital letters (write "Hp" not "HP", "Dell" not "DELL", "Msi" not "MSI"). Exception: acronyms in CPU/GPU names.
- "cpu": use the commercial name. Keep the hyphen only for Intel iX series (i5-8350u, i7-1165G7) and AMD Ryzen series. Write lowercase after the hyphen.
- "gpu": use the commercial name. Replace dashes with spaces (write "Rtx 3050 Ti" not "RTX-3050-Ti"). Keep only the first letter capitalized for brand prefixes (Gtx, Rtx, Mx).
- "gpu_type": one of "Integrated", "Dedicated", or null.
- "ram", "storage", "gpu_vram": numbers in GB (1 TB = 1000 GB).
- "ssd": 1 if SSD, 0 if HDD, null if unknown.
- "screen_size": number in inches (e.g. 15.6), null if unknown.
- "refresh_rate": number in Hz (e.g. 120), null if unknown.
- "new": 1 if brand new/sealed, 0 if used/occasion, null if unclear.
- "touchscreen": 1 if touchscreen/tactile mentioned, 0 if not, null if unclear.
- If a spec cannot be determined from the description, use null.
- Return ONLY the JSON array, no markdown, no backticks, no explanation.

Here are 3 reference examples showing the exact output format:
{EXAMPLES}"""


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------
def _sb_headers(prefer="return=minimal"):
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def fetch_new_laptops_batch(limit: int = GEMINI_BATCH) -> list[dict]:
    """Fetch a batch of rows from new_laptops."""
    url = (
        f"{SUPABASE_URL}/rest/v1/new_laptops"
        f"?select=*"
        f"&order=id.asc"
        f"&limit={limit}"
    )
    resp = requests.get(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    resp.raise_for_status()
    return resp.json()


def count_new_laptops() -> int:
    """Count remaining rows in new_laptops."""
    url = f"{SUPABASE_URL}/rest/v1/new_laptops?select=id"
    resp = requests.get(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact",
    })
    resp.raise_for_status()
    cr = resp.headers.get("content-range", "*/0")
    total = cr.split("/")[-1]
    return int(total) if total != "*" else 0


def delete_from_new_laptops(ids: list[int]):
    """Delete processed rows from new_laptops by their id."""
    if not ids:
        return
    ids_param = ",".join(str(i) for i in ids)
    url = f"{SUPABASE_URL}/rest/v1/new_laptops?id=in.({ids_param})"
    resp = requests.delete(url, headers=_sb_headers())
    if not resp.ok:
        logger.error("Delete from new_laptops failed: %d %s", resp.status_code, resp.text[:200])


def upsert_to_laptops(rows: list[dict]):
    """Upsert rows into the laptops table (on_conflict=avito_id)."""
    url = f"{SUPABASE_URL}/rest/v1/laptops?on_conflict=avito_id"
    resp = requests.post(url, headers=_sb_headers("return=minimal,resolution=merge-duplicates"), json=rows)
    if not resp.ok:
        logger.error("Upsert failed: %d %s", resp.status_code, resp.text[:300])
        return False
    return True


# ---------------------------------------------------------------------------
# Gemini parsing
# ---------------------------------------------------------------------------
def build_prompt(items: list[dict]) -> str:
    lines = []
    for i, item in enumerate(items):
        desc = item.get("description", "")
        lines.append(f"{i+1}. {desc}")
    return "Extract specs from these laptop descriptions:\n\n" + "\n".join(lines)


def parse_batch_gemini(client, items: list[dict], retries: int = 3) -> list[dict]:
    """Send a batch to Gemini and parse the JSON response."""
    prompt = build_prompt(items)

    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=[
                    {"role": "user", "parts": [{"text": SYSTEM_PROMPT}]},
                    {"role": "user", "parts": [{"text": prompt}]},
                ],
            )

            text = response.text.strip()

            # Strip markdown fences
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            parsed = json.loads(text)

            if not isinstance(parsed, list):
                logger.error("Expected list, got %s", type(parsed))
                return [{}] * len(items)

            while len(parsed) < len(items):
                parsed.append({})
            parsed = parsed[:len(items)]

            return parsed

        except json.JSONDecodeError as e:
            logger.error("JSON parse error (attempt %d): %s", attempt + 1, e)
            logger.error("Raw response (first 300 chars): %s", text[:300])
        except Exception as e:
            logger.error("API error (attempt %d): %s", attempt + 1, e)

        if attempt < retries - 1:
            wait = 15 * (attempt + 1)
            logger.info("Retrying in %ds...", wait)
            time.sleep(wait)

    return [{}] * len(items)


# ---------------------------------------------------------------------------
# Scoring & row building
# ---------------------------------------------------------------------------
def truncate_description(desc: str, target: int = 80) -> str:
    if not desc or len(desc) <= target:
        return desc
    idx = desc.find(" ", target)
    if idx == -1:
        return desc
    return desc[:idx]


def to_db_row(raw_item: dict, specs: dict) -> dict:
    """Merge raw scraped data + parsed specs + score into a DB row."""
    out = {}

    # Metadata from scraped item
    out["avito_id"] = str(raw_item.get("avito_id", ""))
    out["description"] = truncate_description(str(raw_item.get("description", "")))
    out["link"] = str(raw_item.get("link", ""))
    out["city"] = str(raw_item.get("city", ""))

    # Price
    try:
        out["price"] = float(raw_item.get("price", 0) or 0)
    except (ValueError, TypeError):
        out["price"] = None

    # Booleans
    for col in BOOL_COLS:
        val = raw_item.get(col, False)
        if isinstance(val, bool):
            out[col] = val
        elif str(val).lower() in ("true", "1", "yes"):
            out[col] = True
        else:
            out[col] = False

    # Specs from Gemini
    for col in SPEC_COLS:
        val = specs.get(col)
        if val is None or val == "null" or val == "":
            if col in NUMERIC_COLS:
                out[col] = None
            else:
                out[col] = ""
        elif col in NUMERIC_COLS:
            try:
                out[col] = float(val)
            except (ValueError, TypeError):
                out[col] = None
        else:
            out[col] = str(val)

    out["is_sold"] = False  # Active item
    out["score"] = calc_laptop_score(out)

    return out


def is_valid_parse(specs: dict) -> bool:
    """Check that Gemini actually extracted something useful."""
    brand = specs.get("brand")
    cpu = specs.get("cpu")
    model = specs.get("model")
    return bool(brand) or bool(cpu) or bool(model)


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------
def main(max_items: int = None):
    if not GEMINI_KEY:
        print("Error: GEMINI_API_KEY not found in .env")
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL/SUPABASE_KEY not found in .env")
        return

    total = count_new_laptops()
    if total == 0:
        print("new_laptops table is empty. Nothing to process.")
        return

    if max_items and total > max_items:
        total = max_items
    print(f"Processing {total} items from new_laptops table...")

    logger.info("Loading Gemini client...")
    gemini_client = genai.Client(api_key=GEMINI_KEY)
    logger.info("Gemini client loaded.")

    processed = 0
    failed_ids = set()
    max_loops = (total // GEMINI_BATCH + 1) * MAX_RETRIES

    for loop in range(max_loops):
        batch = fetch_new_laptops_batch(GEMINI_BATCH)
        if not batch:
            break

        fresh_batch = [item for item in batch if item["id"] not in failed_ids]
        if not fresh_batch:
            logger.warning("All remaining %d items failed parsing. Stopping.", len(batch))
            break

        logger.info("--- Batch %d: %d items ---", loop + 1, len(fresh_batch))

        # Parse with Gemini
        specs_list = parse_batch_gemini(gemini_client, fresh_batch)

        # Separate valid and invalid parses
        valid_items = []
        valid_specs = []

        for item, specs in zip(fresh_batch, specs_list):
            if is_valid_parse(specs):
                valid_items.append(item)
                valid_specs.append(specs)
            else:
                failed_ids.add(item["id"])
                logger.warning("Failed to parse item id=%d avito_id=%s", item["id"], item.get("avito_id"))

        if not valid_items:
            logger.warning("No valid parses in this batch. Continuing...")
            time.sleep(5)
            continue

        # Build DB rows (no embeddings needed)
        db_rows = []
        processed_ids = []
        seen_links = set()
        seen_avito_ids = set()

        for item, specs in zip(valid_items, valid_specs):
            link = item.get("link")
            avito_id = str(item.get("avito_id", ""))

            # Skip duplicates within the same batch to prevent ON CONFLICT 21000 errors
            if link in seen_links or avito_id in seen_avito_ids:
                logger.warning("Skipping duplicate in batch: avito_id=%s", avito_id)
                processed_ids.append(item["id"])  # delete from staging anyway
                continue

            seen_links.add(link)
            seen_avito_ids.add(avito_id)

            db_row = to_db_row(item, specs)
            db_rows.append(db_row)
            processed_ids.append(item["id"])

        # Upsert into laptops
        success = upsert_to_laptops(db_rows)
        if success:
            delete_from_new_laptops(processed_ids)
            processed += len(processed_ids)
            remaining = count_new_laptops()
            logger.info("Upserted %d items. Total processed: %d. Remaining: %d",
                        len(processed_ids), processed, remaining)
        else:
            logger.error("Upsert failed for batch, items remain in new_laptops for retry.")

        # Rate limit: Gemini free tier (15 RPM)
        time.sleep(10)

        if max_items and processed >= max_items:
            break

    remaining = count_new_laptops()
    print(f"\nProcessing complete!")
    print(f"  Successfully processed: {processed}")
    print(f"  Remaining in new_laptops: {remaining}")
    if failed_ids:
        print(f"  Failed items (ids): {sorted(failed_ids)}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse and upload new laptops")
    parser.add_argument("-n", "--num", type=int, default=None,
                        help="Max items to process (default: all)")
    args = parser.parse_args()
    main(args.num)
