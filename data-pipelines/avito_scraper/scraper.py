# -*- coding: utf-8 -*-
"""
Avito Laptop Scraper
Fetches laptop listings from Avito.ma, compresses text, outputs CSV.
"""
import re
import csv
import json
import random
import asyncio
import logging
import aiohttp
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
import unicodedata

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://www.avito.ma/fr/maroc/ordinateurs_portables"
MAX_PAGES = 500
BATCH_SIZE = 10
BATCH_DELAY = 2.0
REQUEST_TIMEOUT = 15
OUTPUT_DIR = Path(__file__).parent / "data"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

CSV_COLUMNS = [
    "avito_id", "description", "price", "city", "link",
    "is_shop", "has_delivery",
    # --- Empty spec columns (filled later by the parser) ---
    "brand", "model", "cpu", "ram", "storage", "ssd",
    "gpu", "gpu_type", "gpu_vram",
    "screen_size", "refresh_rate", "new", "touchscreen",
]

# ---------------------------------------------------------------------------
# Text compression
# ---------------------------------------------------------------------------
def compress_text(title: str, description: str) -> str:
    """Join title + description, normalize to ASCII, keep only basic letters/digits/dots/spaces,
    collapse runs of 3+ identical chars to one."""
    text = f"{title} {description}".lower()
    
    # Normalize unicode to remove accents (é -> e) and strip non-ASCII / ambiguous characters
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    
    # Keep strictly basic english letters, numbers, spaces, and dots
    text = re.sub(r"[^a-z0-9\s.]", " ", text)
    
    # Collapse 3+ identical consecutive characters -> 1
    text = re.sub(r"(.)\1{2,}", r"\1", text)
    
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Referer": "https://www.avito.ma/",
        "DNT": "1",
    }

def _extract_next_data(html: str) -> dict | None:
    """Pull the __NEXT_DATA__ JSON blob from the page."""
    if "__NEXT_DATA__" not in html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        return json.loads(tag.string)
    if tag:
        content = tag.get_text()
        if content:
            return json.loads(content)
    # Regex fallback
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    return None

def _parse_ads(data: dict) -> list[dict]:
    """Extract raw ad dicts from the parsed JSON."""
    ads = (
        data.get("props", {})
        .get("pageProps", {})
        .get("componentProps", {})
        .get("ads", {})
        .get("ads", [])
    )
    results = []
    for ad in ads:
        try:
            price_data = ad.get("price", {})
            price = float(price_data.get("value", 0)) if isinstance(price_data, dict) else float(price_data or 0)

            link = ad.get("href", "")
            if link and not link.startswith("http"):
                link = f"https://www.avito.ma{link}"

            avito_id = ad.get("id")
            if not avito_id and link:
                m = re.search(r"_(\d+)\.htm", link)
                if m:
                    avito_id = m.group(1)

            title = ad.get("subject", "")
            desc = ad.get("description", "")
            if not title:
                continue

            results.append({
                "avito_id": avito_id or "",
                "description": compress_text(title, desc),
                "price": price,
                "city": ad.get("location", ""),
                "link": link,
                "is_shop": ad.get("isShop", False),
                "has_delivery": ad.get("hasShipping", False) or ad.get("isDelivery", False),
            })
        except Exception:
            logger.debug("Skipped malformed ad", exc_info=True)
    return results

# ---------------------------------------------------------------------------
# Async scraping engine
# ---------------------------------------------------------------------------
async def _fetch_page(session: aiohttp.ClientSession, page: int) -> list[dict]:
    url = f"{BASE_URL}?o={page}"
    try:
        async with session.get(url, headers=_headers(), timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                logger.warning("Page %d: HTTP %d", page, resp.status)
                return []
            html = await resp.text()
            data = _extract_next_data(html)
            if not data:
                logger.warning("Page %d: no __NEXT_DATA__", page)
                return []
            ads = _parse_ads(data)
            logger.info("Page %d: %d ads", page, len(ads))
            return ads
    except asyncio.TimeoutError:
        logger.warning("Page %d: timeout", page)
        return []
    except aiohttp.ClientError as e:
        logger.warning("Page %d: %s", page, e)
        return []

async def scrape(max_pages: int = MAX_PAGES) -> list[dict]:
    """Scrape `max_pages` pages and return de-duplicated listings."""
    all_ads: list[dict] = []
    seen: set[str] = set()

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        for batch_start in range(1, max_pages + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, max_pages + 1)
            tasks = [_fetch_page(session, p) for p in range(batch_start, batch_end)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, list):
                    for ad in result:
                        key = ad["link"] or ad["avito_id"]
                        if key and key not in seen:
                            seen.add(key)
                            all_ads.append(ad)

            done = min(batch_end - 1, max_pages)
            logger.info("Progress: %d/%d pages | %d unique ads", done, max_pages, len(all_ads))

            if batch_end <= max_pages:
                await asyncio.sleep(BATCH_DELAY)

    logger.info("Scraping complete: %d unique ads", len(all_ads))
    return all_ads

# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------
def save_csv(ads: list[dict], path: Path | None = None) -> Path:
    """Write ads to CSV with empty spec columns."""
    if path is None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUTPUT_DIR / f"laptops_{datetime.now():%Y%m%d_%H%M}.csv"

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for ad in ads:
            row = {col: ad.get(col, "") for col in CSV_COLUMNS}
            writer.writerow(row)

    logger.info("Saved %d rows to %s", len(ads), path)
    return path

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(max_pages: int = MAX_PAGES):
    start = datetime.now()
    ads = asyncio.run(scrape(max_pages))
    path = save_csv(ads)
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\nDone: {len(ads)} laptops saved to {path} in {elapsed:.1f}s")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Avito Laptop Scraper")
    parser.add_argument("-p", "--pages", type=int, default=MAX_PAGES, help="Number of pages to scrape (default: 500)")
    args = parser.parse_args()
    main(args.pages)
