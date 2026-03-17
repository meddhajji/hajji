import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

print("=== PIPELINE VERIFICATION ===")

# 1. Check pipeline_stats
resp = requests.get(f"{SUPABASE_URL}/rest/v1/pipeline_stats?order=created_at.desc&limit=1", headers=headers)
stats = resp.json()[0] if resp.ok and resp.json() else {}
print(f"\nLatest Pipeline Stats:")
print(f"  Created At: {stats.get('created_at')}")
print(f"  New Items:  {stats.get('new_items')}")

# 2. Check laptop count (total and active)
resp = requests.get(f"{SUPABASE_URL}/rest/v1/laptops?select=id", headers={**headers, "Prefer": "count=exact"})
total = int(resp.headers.get("content-range", "*/0").split("/")[-1]) if resp.ok else 0

resp = requests.get(f"{SUPABASE_URL}/rest/v1/laptops?select=id&is_sold=eq.false", headers={**headers, "Prefer": "count=exact"})
active = int(resp.headers.get("content-range", "*/0").split("/")[-1]) if resp.ok else 0

print(f"\nLaptops Table:")
print(f"  Total Laptops:  {total}")
print(f"  Active Laptops: {active}")
print(f"  Sold Laptops:   {total - active}")

# 3. Check recently added/parsed laptops
print("\nRecently Parsed Laptops (Sample of 3):")
resp = requests.get(f"{SUPABASE_URL}/rest/v1/laptops?order=updated_at.desc&limit=3", headers=headers)
if resp.ok:
    for row in resp.json():
        print(f"\n- Brand/Model: {row.get('brand')} {row.get('model')}")
        print(f"  CPU: {row.get('cpu')} | RAM: {row.get('ram')}GB | Storage: {row.get('storage')}GB (SSD: {row.get('ssd')})")
        print(f"  GPU: {row.get('gpu')} | Screen: {row.get('screen_size')}\" {row.get('refresh_rate')}Hz")
        print(f"  Price: {row.get('price')} DH | City: {row.get('city')}")
        print(f"  Score: {row.get('score')} | is_sold: {row.get('is_sold')}")
        print(f"  Desc: {row.get('description')}")
        
# 4. Check new_laptops table
resp = requests.get(f"{SUPABASE_URL}/rest/v1/new_laptops?select=id", headers={**headers, "Prefer": "count=exact"})
new_laptops = int(resp.headers.get("content-range", "*/0").split("/")[-1]) if resp.ok else 0
print(f"\nItems remaining in new_laptops staging: {new_laptops}")
