import json
import requests
import time
import random
import gc

with open("total.json") as f:
    models = json.load(f)
for model in models:
    print(model)
    print('\n')
    for listing in model:
        print(listing)
        break
    break