import json
import requests
import time
import random
import gc
from bs4 import BeautifulSoup


with open("model_ids.json") as f:
    models = json.load(f)

def extract_listing_details(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the <strong>Listing Details</strong>
    listing_header = soup.find("strong", string="Listing Details")
    if not listing_header:
        return "⚠️ Could not find 'Listing Details' on this page."

    # Find the <ul> that comes right after
    ul = listing_header.find_next("ul")
    if not ul:
        return "⚠️ No <ul> found after 'Listing Details'."

    # Extract plain text from <li> items
    items = []
    for li in ul.find_all("li"):
        text = li.get_text(strip=True)
        if text:
            items.append(f"- {text}")

    return "\n".join(items)

import time, random, requests, gc

def fetch_page(url):    
    time.sleep(random.uniform(0.1, 0.3))  # initial random delay
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        sold = []
        items = []

        # Check for stats -> sold
        stats = data.get("stats")
        if stats and isinstance(stats, dict):
            sold = stats.get("sold", []) or []

        # Check for items
        items = data.get("items", []) or []

        print(f"\tSold: {len(sold)}")
        val = 1
        for item in sold:
            print(f"\tOn item {val} of {len(sold)}")
            val += 1
            try:
                # Prefer permalink if available
                target_url = item.get("permalink") or item.get("url")
                item["details"] = extract_listing_details(target_url)
            except Exception as e:
                print(f"\t⚠️ Failed to get details for sold item: {e}")
                item["details"] = "fail"
            time.sleep(random.uniform(0.05, 0.1))

        print(f"\tItems: {len(items)}")
        val = 1
        for item in items:
            print(f"\tOn item {val} of {len(items)}")
            val += 1
            try:
                target_url = item.get("url")
                item["details"] = extract_listing_details(target_url)
            except Exception as e:
                print(f"\t⚠️ Failed to get details for item: {e}")
                item["details"] = "fail"
            time.sleep(random.uniform(0.05, 0.1))

        # Clean up memory
        del r, data, val
        gc.collect()

        return sold + items
    
    except Exception as e:
        print(f"FAILED: {e}")
        return ["fail"]
val = 1

all_models_data = {}

for model in models:
    print(f"\nProcessing {model} ({val} of {len(models)})")
    url = (
        "https://bringatrailer.com/wp-json/bringatrailer/1.0/data/listings-filter?"
        "page=1&get_items=1"
        f"&base_filter[keyword_pages][]={model}"
        "&base_filter[items_type]=model"
        "&sort=td"
    )

    # Fetch data for this model
    model_data = fetch_page(url)
    all_models_data[model] = model_data

    # Write to file after each model
    with open("total.json", "w", encoding="utf-8") as f:
        json.dump(all_models_data, f, ensure_ascii=False, indent=2)

    # Clear memory
    del model_data
    gc.collect()

    print(f"✅ Saved {model} to total.json, number {val} of {len(models)} models")
    val += 1

print("✅ Saved all data to total.json")