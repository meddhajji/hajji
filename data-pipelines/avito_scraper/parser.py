# -*- coding: utf-8 -*-
"""
parser.py
Parses raw Avito laptop descriptions into structured specs using Gemini API.
Reads new.csv (from refresh.py), processes in batches of 400, outputs parsed.csv.

Usage:
    python parser.py                 # parse all items in data/new.csv
    python parser.py -n 50           # parse only the first 50 items (test)
"""

import os
import csv
import json
import time
import logging
from pathlib import Path

from dotenv import load_dotenv
from google import genai

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GEMINI_KEY = os.getenv("GEMINI_API_KEY")
MODEL = "gemini-3.1-flash-lite-preview"
API_CHUNK = 50       # items per Gemini API call (keep small for reliability)
REPORT_EVERY = 100   # print progress every N items
INPUT_FILE = Path(__file__).parent / "data" / "new.csv"
OUTPUT_FILE = Path(__file__).parent / "data" / "parsed.csv"

# Columns that Gemini must extract (the spec columns)
SPEC_COLS = [
    "brand", "model", "cpu", "ram", "storage", "ssd",
    "gpu", "gpu_type", "gpu_vram",
    "screen_size", "refresh_rate", "new", "touchscreen",
]

# Full output columns matching good.csv exactly
ALL_COLS = [
    "avito_id", "description", "price", "city", "link",
    "is_shop", "has_delivery",
    "brand", "model", "cpu", "ram", "storage", "ssd",
    "gpu", "gpu_type", "gpu_vram",
    "screen_size", "refresh_rate", "new", "touchscreen",
]

# 3 reference examples from good.csv showing the exact format we want
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


def build_prompt(descriptions: list[dict]) -> str:
    """Build the user prompt with numbered descriptions."""
    lines = []
    for i, item in enumerate(descriptions):
        desc = item.get("description", "")
        lines.append(f"{i+1}. {desc}")
    return "Extract specs from these laptop descriptions:\n\n" + "\n".join(lines)


def parse_batch(client, descriptions: list[dict], retries: int = 3) -> list[dict]:
    """Send a batch to Gemini and parse the JSON response. Retries on failure."""
    prompt = build_prompt(descriptions)

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

            # Strip markdown fences if the model wraps them
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            parsed = json.loads(text)

            if not isinstance(parsed, list):
                logger.error("Expected list, got %s", type(parsed))
                return [{}] * len(descriptions)

            # Pad or trim to match input length
            while len(parsed) < len(descriptions):
                parsed.append({})
            parsed = parsed[:len(descriptions)]

            return parsed

        except json.JSONDecodeError as e:
            logger.error("JSON parse error (attempt %d): %s", attempt + 1, e)
            logger.error("Raw response (first 300 chars): %s", text[:300])
        except Exception as e:
            logger.error("API error (attempt %d): %s", attempt + 1, e)

        if attempt < retries - 1:
            wait = 5 * (attempt + 1)
            logger.info("Retrying in %ds...", wait)
            time.sleep(wait)

    return [{}] * len(descriptions)


def main(max_items: int = None):
    if not GEMINI_KEY:
        print("Error: GEMINI_API_KEY not found in .env")
        return

    client = genai.Client(api_key=GEMINI_KEY)

    # Read input
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found. Run refresh.py first.")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_items = list(reader)

    if max_items:
        all_items = all_items[:max_items]

    total = len(all_items)
    total_chunks = (total + API_CHUNK - 1) // API_CHUNK
    print(f"Parsing {total} items ({total_chunks} API calls of {API_CHUNK} each)...")
    print(f"Model: {MODEL}")
    print()

    results = []
    done = 0

    for chunk_start in range(0, total, API_CHUNK):
        chunk_end = min(chunk_start + API_CHUNK, total)
        chunk = all_items[chunk_start:chunk_end]
        chunk_num = chunk_start // API_CHUNK + 1

        try:
            specs_list = parse_batch(client, chunk)
        except Exception as e:
            logger.error("Chunk %d failed permanently: %s", chunk_num, e)
            specs_list = [{}] * len(chunk)

        # Merge scraped metadata with parsed specs
        for item, specs in zip(chunk, specs_list):
            row = {}
            # Copy metadata columns from the scraped item
            for col in ["avito_id", "description", "price", "city", "link",
                         "is_shop", "has_delivery"]:
                row[col] = item.get(col, "")
            # Copy spec columns from Gemini output
            for col in SPEC_COLS:
                val = specs.get(col)
                if val is None or val == "null":
                    row[col] = ""
                else:
                    row[col] = val
            results.append(row)

        done += len(chunk)
        if done % REPORT_EVERY < API_CHUNK or done == total:
            pct = done / total * 100
            logger.info("Progress: %d/%d items (%.0f%%)", done, total, pct)

        # Rate limit: 15 RPM = 4s between calls, use 5s for safety
        if chunk_end < total:
            time.sleep(5)

    # Write output
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_COLS, extrasaction="ignore")
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"\nDone! Saved {len(results)} parsed items to {OUTPUT_FILE}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse Avito laptop descriptions with Gemini")
    parser.add_argument("-n", "--num", type=int, default=None,
                        help="Max items to parse (default: all)")
    args = parser.parse_args()
    main(args.num)
