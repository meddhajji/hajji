# -*- coding: utf-8 -*-
"""
pipeline.py

The master script that runs the entire daily Avito data refresh pipeline.
It executes the following steps in sequence:
1. refresh.py       (Scrape Avito, diff with Supabase, write new.csv)
2. parser.py        (Use Gemini to extract specs from new.csv -> parsed.csv)
3. upload.py        (Generate embeddings, score, and upsert parsed data to Supabase)

Usage:
    python pipeline.py
"""

import sys
import time
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def run_step(script_name: str, args: list = None):
    """Run a python script and return its execution time."""
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    logger.info("=" * 60)
    logger.info("STARTING: %s %s", script_name, " ".join(args or []))
    logger.info("=" * 60)
    
    start_time = time.time()
    result = subprocess.run(cmd, cwd=str(Path(__file__).parent))
    elapsed = time.time() - start_time
    
    if result.returncode != 0:
        logger.error("%s failed with exit code %d", script_name, result.returncode)
        sys.exit(result.returncode)
        
    logger.info("%s finished in %.1fs\n", script_name, elapsed)
    return elapsed

def main():
    total_start = time.time()
    logger.info("Starting Daily Avito Pipeline")
    
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    # Step 1: Scrape & Diff (generate new.csv)
    # Using 500 pages as requested for a thorough refresh
    t1 = run_step("refresh.py", ["-p", "500"])
    
    # Check if there's anything to parse
    new_csv = data_dir / "new.csv"
    has_new_data = False
    if new_csv.exists():
        with open(new_csv, "r", encoding="utf-8") as f:
            lines = f.readlines()
            if len(lines) > 1:  # More than just the header
                has_new_data = True
                
    if has_new_data:
        # Step 2: Parse raw descriptions into specs (generate parsed.csv)
        t2 = run_step("parser.py")
        
        # Step 3: Embed, Score & Upload to Supabase
        # upload.py calculates scores inline before upserting
        t3 = run_step("upload.py")
    else:
        logger.info("No new or updated items found. Skipping Parser and Upload steps.")
        t2 = t3 = 0.0
    
    total_time = time.time() - total_start
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info("Refresh:  %.1fs", t1)
    logger.info("Parse:    %.1fs", t2)
    logger.info("Upload:   %.1fs", t3)
    logger.info("Total:    %.1fs (%.1f mins)", total_time, total_time / 60)
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
