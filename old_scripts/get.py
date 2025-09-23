import requests
import json
import random
import time

# Random user agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/119 Safari/537.36",
]

items = []

def fetch_page(page_num):
    headers = {
        "User-Agent": random.choice(user_agents)
    }
    url = f"https://bringatrailer.com/wp-json/bringatrailer/1.0/data/listings-filter?page={page_num}&per_page=50&get_items=1&get_stats=1&sort=td"
    
    start_time = time.perf_counter()
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        elapsed = time.perf_counter() - start_time
        print(f"Page {page_num} fetched in {elapsed:.2f}s")
        data = r.json()
        return data.get("stats", {}).get("sold", [])
    except Exception as e:
        elapsed = time.perf_counter() - start_time
        print(f"Page {page_num} failed after {elapsed:.2f}s: {e}")
        return []

# Create a shuffled list of pages
pages = list(range(200, 400))  # 0–400 inclusive
random.shuffle(pages)

# Loop over shuffled pages
for page_num in pages:
    page_items = fetch_page(page_num)
    items += page_items
    print(f"Total items so far: {len(items)}")
    # wait = random.uniform(1, 3)
    # print(f"Waiting {wait:.2f}s before next request...")
    # time.sleep(wait)

# Save results
with open("items.json", "w", encoding="utf-8") as f:
    json.dump(items, f, indent=2)

print(f"Done! Collected {len(items)} items")
