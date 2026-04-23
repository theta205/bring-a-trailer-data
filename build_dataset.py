"""
Builds the full car_auction_data.csv from listings.db + total.json.
- listings.db  : id, model, year, mileage, transmission, timestamp_end, country_code
- total.json   : current_bid (price), title, details (fallback parsing for nulls)
Computes: is_manual, is_us, auction_year, auction_month, age, log_price, log_mileage
"""

import json
import re
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timezone

DB_PATH = "data/listings.db"
TOTAL_PATH = "data/total.json"
OUT_PATH = "data/processed/car_auction_data.csv"

# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_year_from_title(title: str):
    if not title:
        return None
    m = re.search(r'\b(1[89]\d\d|20[012]\d)\b', str(title))
    return int(m.group(1)) if m else None

def parse_mileage_from_details(details: str):
    if not details:
        return None
    text = str(details)
    if re.search(r'mileage unknown|tmu|no mileage', text, re.I):
        return None
    # "123,456 Miles" or "123k Miles" or "123K Miles"
    m = re.search(r'([\d,]+)[kK]\s*[Mm]iles?', text)
    if m:
        return int(m.group(1).replace(',', '')) * 1000
    m = re.search(r'([\d,]+)\s*[Mm]iles?', text)
    if m:
        val = int(m.group(1).replace(',', ''))
        return val if val < 2_000_000 else None
    return None

def parse_transmission_from_details(details: str):
    if not details:
        return None
    text = str(details).lower()
    if re.search(r'\bautomatic\b|\bauto\s+trans|\b\d-speed\s+auto|\bpdk\b|\bdct\b|\bdsg\b|\bcvt\b|\btiptronic\b|\bsmoothtronic\b|\bpowershift\b', text):
        return 'Automatic'
    if re.search(r'\bmanual\b|\bmanual\s+trans|\b\d-speed\s+manual|\b\d-speed\s+gearbox|\b\d-speed\s+transaxle|\bmanual\s+gearbox', text):
        return 'Manual'
    return None

# ── 1. Load all rows from listings.db ────────────────────────────────────────
print("Loading listings.db...")
conn = sqlite3.connect(DB_PATH)
db = pd.read_sql_query(
    "SELECT id, model, year, transmission, mileage, timestamp_end, country_code, current_bid FROM listings",
    conn,
)
conn.close()
print(f"  {len(db):,} rows in DB")

# ── 2. Load total.json ────────────────────────────────────────────────────────
print("Loading total.json...")
with open(TOTAL_PATH) as f:
    raw = json.load(f)

seen_ids = {}
for items in raw.values():
    for item in items:
        if not isinstance(item, dict):
            continue
        iid = item.get("id")
        if iid is None:
            continue

        # Car listing entry (has current_bid / active)
        bid = item.get("current_bid")
        # Bid history entry (has amount) — use as fallback price
        amount = item.get("amount")

        # Bid history events use "timestamp" instead of "timestamp_end"
        ts = item.get("timestamp_end") or item.get("timestamp")

        if iid not in seen_ids:
            seen_ids[iid] = {
                "current_bid": bid,
                "amount": amount,
                "country_code": item.get("country_code"),
                "timestamp_end": ts,
                "title": item.get("title"),
                "details": item.get("details"),
            }
        else:
            prev_bid = seen_ids[iid]["current_bid"]
            prev_amt = seen_ids[iid]["amount"]
            # Keep highest valid current_bid
            try:
                if bid and float(bid) > float(prev_bid or 0):
                    seen_ids[iid]["current_bid"] = bid
            except (TypeError, ValueError):
                pass
            # Keep highest valid amount (last/winning bid timestamp is best proxy for auction end)
            try:
                if amount and float(amount) > float(prev_amt or 0):
                    seen_ids[iid]["amount"] = amount
                    if ts:
                        seen_ids[iid]["timestamp_end"] = ts
            except (TypeError, ValueError):
                pass
            # Fill in missing metadata
            for field in ("country_code", "title", "details"):
                if not seen_ids[iid].get(field) and item.get(field):
                    seen_ids[iid][field] = item[field]
            if not seen_ids[iid].get("timestamp_end") and ts:
                seen_ids[iid]["timestamp_end"] = ts

tj = pd.DataFrame.from_dict(seen_ids, orient="index").reset_index()
tj = tj.rename(columns={"index": "id"})
tj["id"] = tj["id"].astype(int)
print(f"  {len(tj):,} unique IDs in total.json")

# ── 3. Merge ──────────────────────────────────────────────────────────────────
df = db.merge(tj, on="id", how="left", suffixes=("_db", "_tj"))

# Coerce False/0 to NaN so combine_first falls through to the next source
def clean_bid(s):
    return pd.to_numeric(s, errors="coerce").replace(0, float("nan"))

df["price"] = clean_bid(df["current_bid_tj"]).combine_first(
              clean_bid(df["current_bid_db"])).combine_first(
              clean_bid(df["amount"]))
df["country_code"] = df["country_code_tj"].combine_first(df["country_code_db"])
df["timestamp_end"] = df["timestamp_end_tj"].combine_first(df["timestamp_end_db"])

drop_cols = [c for c in ["current_bid_db", "current_bid_tj", "country_code_db",
                          "country_code_tj", "timestamp_end_db", "timestamp_end_tj",
                          "current_bid", "amount"] if c in df.columns]
df = df.drop(columns=drop_cols)

# ── 4. Fill nulls from total.json title/details ───────────────────────────────
print("Filling missing year/mileage/transmission from title+details...")

missing_year = df["year"].isna()
missing_mileage = df["mileage"].isna()
missing_trans = df["transmission"].isna()

df["year"] = df["year"].astype(object)
df["mileage"] = df["mileage"].astype(object)
df.loc[missing_year, "year"] = df.loc[missing_year, "title"].apply(parse_year_from_title)
df.loc[missing_mileage, "mileage"] = df.loc[missing_mileage, "details"].apply(parse_mileage_from_details)
df.loc[missing_trans, "transmission"] = df.loc[missing_trans, "details"].apply(parse_transmission_from_details)

filled_year = missing_year.sum() - df["year"].isna().sum()
filled_mileage = missing_mileage.sum() - df["mileage"].isna().sum()
filled_trans = missing_trans.sum() - df["transmission"].isna().sum()
print(f"  Recovered year: +{filled_year:,}  mileage: +{filled_mileage:,}  transmission: +{filled_trans:,}")

# ── 5. Derived columns ────────────────────────────────────────────────────────

df["is_manual"] = df["transmission"].str.lower().str.contains("manual", na=False).astype(int)
df["is_us"] = (df["country_code"].fillna("").str.upper() == "US").astype(int)

def ts_to_ym(ts):
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return dt.year, f"{dt.month:02d}"
    except Exception:
        return None, None

auction_years, auction_months = zip(*df["timestamp_end"].map(
    lambda ts: ts_to_ym(ts) if pd.notna(ts) else (None, None)
))
df["auction_year"] = auction_years
df["auction_month"] = auction_months
df["age"] = df["auction_year"] - df["year"]
df["log_price"] = np.log1p(df["price"].astype(float).fillna(0))
df["log_mileage"] = np.log1p(df["mileage"].astype(float).fillna(0))

# ── 6. Filter ────────────────────────────────────────────────────────────────
before = len(df)
df = df[df["price"].notna() & (df["price"].astype(float) > 0)]
df = df[df["year"].notna() & (df["year"].astype(float) > 1885) & (df["year"].astype(float) <= 2026)]
df = df[df["mileage"].notna() & (df["mileage"].astype(float) >= 0)]
df = df[df["transmission"].notna()]
df = df[df["model"].notna()]
df = df[df["timestamp_end"].notna()]  # need valid date for auction_year/month
# Remove only extreme data-entry errors (>$10M)
df = df[df["price"].astype(float) <= 10_000_000]

print(f"\nFiltered {before - len(df):,} rows → {len(df):,} usable rows")

# ── 7. Final columns & write ──────────────────────────────────────────────────
df = df[["id", "model", "year", "transmission", "mileage", "price",
         "is_manual", "is_us", "auction_year", "auction_month",
         "age", "log_price", "log_mileage"]]
df = df.sort_values("id").reset_index(drop=True)
df.to_csv(OUT_PATH, index=False)

print(f"Written to {OUT_PATH}")
print(f"\nColumn nulls:\n{df.isnull().sum()}")
print(f"\nPrice stats:\n{df['price'].describe()}")
print(f"\nYear range: {int(df['year'].min())} – {int(df['year'].max())}")
