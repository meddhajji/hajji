# -*- coding: utf-8 -*-
"""
upload.py
Reads parsed.csv and upserts rows into Supabase with:
  - Description truncated to ~80 chars (cut at first space after 80)
  - created_at = now() for new items, unchanged for existing
  - updated_at = now() for all upserted items
  - Embedding vector (BAAI/bge-small-en-v1.5, 384-dim)

Usage:
    python upload.py                 # upload all rows from data/parsed.csv
    python upload.py -n 400          # upload only the first 400 rows
    python upload.py --verify        # read back last uploaded rows and save to verified.csv
"""

import os
import csv
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

from score_laptops import calc_laptop_score

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
PARSED_FILE = Path(__file__).parent / "data" / "parsed.csv"
VERIFIED_FILE = Path(__file__).parent / "data" / "verified.csv"
EMBED_MODEL = "BAAI/bge-small-en-v1.5"

# Columns that go into Supabase
DB_COLS = [
    "avito_id", "description", "price", "city", "link",
    "is_shop", "has_delivery",
    "brand", "model", "cpu", "ram", "storage", "ssd",
    "gpu", "gpu_type", "gpu_vram",
    "screen_size", "refresh_rate", "new", "touchscreen",
]

NUMERIC_COLS = ["price", "ram", "storage", "ssd", "gpu_vram",
                "screen_size", "refresh_rate", "new", "touchscreen"]
BOOL_COLS = ["is_shop", "has_delivery"]


def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }


def truncate_description(desc: str, target: int = 80) -> str:
    """Truncate description at the first space after `target` characters."""
    if not desc or len(desc) <= target:
        return desc
    # Find first space after position target
    idx = desc.find(" ", target)
    if idx == -1:
        return desc  # no space found, keep as-is
    return desc[:idx]


def build_search_text(row: dict) -> str:
    """Build the text used for embedding, matching reembed.mjs logic."""
    parts = [
        row.get("brand") or "",
        row.get("model") or "",
        row.get("cpu") or "",
        (row.get("description") or "")[:200],
    ]
    text = " ".join(p for p in parts if p).strip()
    return text or "laptop"


def to_db_row(row: dict, embedding: list, now_str: str) -> dict:
    """Convert a parsed CSV row into a Supabase-ready dict."""
    out = {}

    for col in DB_COLS:
        val = row.get(col, "")

        if col in NUMERIC_COLS:
            if val == "" or val is None:
                out[col] = None
            else:
                try:
                    out[col] = float(val)
                except (ValueError, TypeError):
                    out[col] = None
        elif col in BOOL_COLS:
            if isinstance(val, bool):
                out[col] = val
            elif str(val).lower() in ("true", "1", "yes"):
                out[col] = True
            elif str(val).lower() in ("false", "0", "no"):
                out[col] = False
            else:
                out[col] = False
        elif col == "description":
            out[col] = truncate_description(str(val)) if val else ""
        else:
            out[col] = str(val) if val else ""

    out["updated_at"] = now_str
    out["embedding"] = json.dumps(embedding)
    out["score"] = calc_laptop_score(out)
    return out


def upsert_batch(rows: list[dict]):
    """Upsert a batch of rows to Supabase using ON CONFLICT (avito_id)."""
    url = f"{SUPABASE_URL}/rest/v1/laptops?on_conflict=avito_id"
    resp = requests.post(url, headers=_sb_headers(), json=rows)
    if not resp.ok:
        logger.error("Upsert failed: %d %s", resp.status_code, resp.text[:300])
        return False
    return True


def verify_uploaded(avito_ids: list[str]):
    """Read back uploaded rows from Supabase and save to verified.csv."""
    logger.info("Verifying %d uploaded rows...", len(avito_ids))
    all_rows = []

    # Fetch in batches of 100 IDs
    for i in range(0, len(avito_ids), 100):
        batch_ids = avito_ids[i:i+100]
        ids_param = ",".join(batch_ids)
        url = f'{SUPABASE_URL}/rest/v1/laptops?avito_id=in.({ids_param})&select=*'
        resp = requests.get(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        if resp.ok:
            all_rows.extend(resp.json())
        else:
            logger.error("Verify fetch failed: %d", resp.status_code)

    if not all_rows:
        logger.error("No rows returned from verification query!")
        return

    # Write to CSV (exclude embedding column - it's huge)
    cols = [c for c in all_rows[0].keys() if c != "embedding"]
    with open(VERIFIED_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow({c: row.get(c, "") for c in cols})

    logger.info("Saved %d verified rows to %s", len(all_rows), VERIFIED_FILE)

    # Quick quality check
    has_brand = sum(1 for r in all_rows if r.get("brand"))
    has_cpu = sum(1 for r in all_rows if r.get("cpu"))
    has_emb = sum(1 for r in all_rows if r.get("embedding"))
    print(f"\nVerification results ({len(all_rows)} rows):")
    print(f"  With brand:     {has_brand}/{len(all_rows)}")
    print(f"  With cpu:       {has_cpu}/{len(all_rows)}")
    print(f"  With embedding: {has_emb}/{len(all_rows)}")
    print(f"  Saved to: {VERIFIED_FILE}")


def main(max_items: int = None, verify_only: bool = False):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL/SUPABASE_KEY not found in .env")
        return

    if verify_only:
        # Just read back and verify the last uploaded batch
        verify_uploaded([])  # We'll handle this differently
        return

    if not PARSED_FILE.exists():
        print(f"Error: {PARSED_FILE} not found. Run parser.py first.")
        return

    # Read parsed items
    with open(PARSED_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        items = list(reader)

    if max_items:
        items = items[:max_items]

    total = len(items)
    print(f"Uploading {total} items to Supabase...")

    # Load embedding model
    logger.info("Loading embedding model (%s)...", EMBED_MODEL)
    model = SentenceTransformer(EMBED_MODEL)
    logger.info("Model loaded.")

    now_str = datetime.now(timezone.utc).isoformat()
    uploaded_ids = []
    batch_size = 50  # Supabase REST API works best with smaller batches

    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch = items[batch_start:batch_end]
        pct = batch_end / total * 100

        # Generate embeddings for this batch
        search_texts = [build_search_text(item) for item in batch]
        embeddings = model.encode(search_texts, normalize_embeddings=True)

        # Build DB rows
        db_rows = []
        for item, emb in zip(batch, embeddings):
            db_row = to_db_row(item, emb.tolist(), now_str)
            db_rows.append(db_row)
            uploaded_ids.append(str(item.get("avito_id", "")))

        # Upsert
        success = upsert_batch(db_rows)
        if success:
            logger.info("Batch %d-%d uploaded (%.0f%% done)",
                        batch_start + 1, batch_end, pct)
        else:
            logger.error("Batch %d-%d FAILED", batch_start + 1, batch_end)

        time.sleep(0.2)  # Small rate limit

    print(f"\nUpload complete! {total} items sent to Supabase.")

    # Verify
    verify_uploaded(uploaded_ids)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Upload parsed items to Supabase")
    parser.add_argument("-n", "--num", type=int, default=None,
                        help="Max items to upload (default: all)")
    args = parser.parse_args()
    main(args.num)
