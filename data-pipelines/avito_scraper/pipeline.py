# -*- coding: utf-8 -*-
"""
pipeline.py

The master script that runs the entire daily Avito data refresh pipeline.
It executes the following steps in sequence:
1. refresh.py       (Scrape Avito, diff with DB by link, populate new_laptops)
2. parser.py        (Loop: parse specs, embed, score, upsert to laptops, clear new_laptops)

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
    
    # Step 1: Scrape & Diff (populate new_laptops table)
    t1 = run_step("refresh.py", ["-p", "500"])
    
    # Step 2: Parse, embed, score & upload (loop until new_laptops is empty)
    t2 = run_step("parser.py")
    
    total_time = time.time() - total_start
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info("Refresh:  %.1fs", t1)
    logger.info("Parse:    %.1fs", t2)
    logger.info("Total:    %.1fs (%.1f mins)", total_time, total_time / 60)
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
