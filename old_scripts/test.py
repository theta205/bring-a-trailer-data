import json
import requests
import time
import random
import gc

with open("./data/total.json") as f:
    data = json.load(f)


# data is a dictionary where keys are model IDs and values are lists of listings
for model_id, listings in data.items():
    print(f"\nModel ID: {model_id}")
    print("-" * 40)
    
    # Print first listing for this model
    if listings:  # Check if there are any listings
        first_listing = listings[0]
        print(parse_title(first_listing.get('title', 'N/A')))
    