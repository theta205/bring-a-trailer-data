import json
import sqlite3
import sys
import traceback
import re
from pathlib import Path

DB_FILE = "../data/listings.db"
LISTINGS_FILE = "../data/total.json"
MODELS_FILE = "../data/models.json"
NOYEAR_FILE = Path("../data/noyear.json")
NO_TRANS_FILE = Path("../data/notrans.json")
NOMILEAGE_FILE = Path("../data/nomileage.json")

YEAR_PATTERN = re.compile(r"\b(19[0-9]{2}|20[0-4][0-9]|2050)\b")
MILEAGE_PATTERN = re.compile(r'([\d,]+(?:\.\d+)?)[ ]?(k|K)?[ ]?(Miles|Kilometers|km|mi|Mi|miles|kilometers)', re.IGNORECASE)

def upsert_listing(cur, listing_data):
    """
    Upsert a listing. If the id exists, only update NULL/missing columns.
    """
    # Check if the id exists
    cur.execute("SELECT * FROM listings WHERE id = ?", (listing_data['id'],))
    existing = cur.fetchone()

    if existing:
        # Build UPDATE dynamically for columns that are None in existing row
        col_names = [desc[0] for desc in cur.description]
        updates = []
        params = []

        for col, val in listing_data.items():
            idx = col_names.index(col)
            if existing[idx] is None and val is not None:
                updates.append(f"{col} = ?")
                params.append(val)

        if updates:
            sql = f"UPDATE listings SET {', '.join(updates)} WHERE id = ?"
            params.append(listing_data['id'])
            cur.execute(sql, params)
        return 0
    else:
        # Insert new row
        columns = ", ".join(listing_data.keys())
        placeholders = ", ".join(["?"] * len(listing_data))
        sql = f"INSERT INTO listings ({columns}) VALUES ({placeholders})"
        cur.execute(sql, tuple(listing_data.values()))
        return 1


def get_transmission(details: str) -> str | None:
    if not details:
        return None
    matches = [(m.group().capitalize(), m.start()) for m in re.finditer(r'\b(Manual|Automatic)\b', details, re.IGNORECASE)]
    if not matches:
        return None
    ref_matches = [m.start() for m in re.finditer(r'\b(transmission|transaxle)\b', details, re.IGNORECASE)]
    if not ref_matches:
        return matches[0][0]
    closest = min(matches, key=lambda x: min(abs(x[1] - ref) for ref in ref_matches))
    return closest[0]

def extract_year(title: str):
    if not title:
        return None
    match = YEAR_PATTERN.search(title)
    if match:
        return int(match.group(0))
    return None

# Matches patterns like: 23k Miles, 21k Kilometers (~13k Miles), 4,500 Miles, 12k mi
MILEAGE_PATTERN = re.compile(
    r"(?i)(?P<number>\d{1,3}(?:[,\.]\d{3})*(?:\.\d+)?)\s*(?P<k>[kK]?)\s*(?:\w*\s*)?(?:Miles|mi|Kilometers|km)\b"
)

def extract_mileage(details: str):
    """
    Extracts mileage in miles from details.
    Returns integer miles or None if not found.
    """
    if not details:
        return None

    matches = list(MILEAGE_PATTERN.finditer(details))
    if not matches:
        return None

    # Prefer first match
    match = matches[0]
    number_str = match.group("number")
    k_indicator = match.group("k")

    if not number_str:
        return None

    try:
        number = float(number_str.replace(',', '').replace('.', ''))
    except ValueError:
        return None

    if k_indicator:
        number *= 1000

    # Convert km to miles if unit is km/kilometer
    unit_text = match.group(0).lower()
    if 'km' in unit_text or 'kilometer' in unit_text:
        number *= 0.621371

    return int(number)

def main():
    try:
        with open(MODELS_FILE, "r", encoding="utf-8") as f:
            models_map = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to read or parse {MODELS_FILE}: {e}")
        sys.exit(1)

    try:
        with open(LISTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to read or parse {LISTINGS_FILE}: {e}")
        sys.exit(1)

    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS listings")
        cur.execute("""
        CREATE TABLE listings (
            id INTEGER PRIMARY KEY,
            group_id TEXT,
            model TEXT,
            details TEXT,
            url TEXT,
            title TEXT,
            year INTEGER,
            transmission TEXT,
            mileage INTEGER,
            timestamp_end BIGINT,
            thumbnail_url TEXT,
            excerpt TEXT,
            country_code TEXT,
            current_bid INTEGER
        )
        """)

        inserted, failed, year_misses, mileage_misses = 0, 0, 0, 0
        missing_year_titles = []
        all_noyear_titles = []
        missing_trans_details = []
        all_notrans_details = []
        missing_mileage_details = []
        all_nomileage_details = []

        for group_id, listings in data.items():
            if not isinstance(listings, list):
                print(f"[WARN] Skipped group {group_id}: not a list")
                continue

            model_name = models_map.get(group_id, "Unknown")

            for listing in listings:
                if not isinstance(listing, dict):
                    print(f"[WARN] Skipped entry in {group_id}: {listing}")
                    failed += 1
                    continue

                try:
                    title = listing.get("title")
                    details = listing.get("details")
                    year = extract_year(title)
                    transmission = get_transmission(details)
                    mileage = extract_mileage(details)

                    if year is None:
                        year_misses += 1
                        all_noyear_titles.append(title)
                        if len(missing_year_titles) < 5:
                            missing_year_titles.append(title)

                    if transmission is None:
                        all_notrans_details.append(details)
                        if len(missing_trans_details) < 5:
                            missing_trans_details.append(details)

                    if mileage is None:
                        mileage_misses += 1
                        all_nomileage_details.append(details)
                        if len(missing_mileage_details) < 5:
                            missing_mileage_details.append(details)

                    listing_data = {
                        "id": listing.get("id"),
                        "group_id": group_id,
                        "model": model_name,
                        "details": details,
                        "url": listing.get("url"),
                        "title": title,
                        "year": year,
                        "transmission": transmission,
                        "mileage": mileage,
                        "timestamp_end": listing.get("timestamp_end"),
                        "thumbnail_url": listing.get("thumbnail_url"),
                        "excerpt": listing.get("excerpt"),
                        "country_code": listing.get("country_code"),
                        "current_bid": listing.get("current_bid"),
                    }

                    inserted += upsert_listing(cur, listing_data)
                    
                except Exception as inner_e:
                    print(f"[WARN] Skipped record {listing.get('id')}: {inner_e}")
                    traceback.print_exc(limit=1)
                    failed += 1

        conn.commit()
        conn.close()

        NOYEAR_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(NOYEAR_FILE, "w", encoding="utf-8") as f:
            json.dump(all_noyear_titles, f, ensure_ascii=False, indent=2)
        with open(NO_TRANS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_notrans_details, f, ensure_ascii=False, indent=2)
        with open(NOMILEAGE_FILE, "w", encoding="utf-8") as f:
            json.dump(all_nomileage_details, f, ensure_ascii=False, indent=2)

        if missing_year_titles:
            print("\nExamples of titles without a year:")
            for t in missing_year_titles:
                print(" -", t)
        if missing_trans_details:
            print("\nExamples of details without transmission info:")
            for d in missing_trans_details:
                print(" -", d)
        if missing_mileage_details:
            print("\nExamples of details without mileage info:")
            for d in missing_mileage_details:
                print(" -", d)

        print(f"[DONE] Inserted: {inserted}, Skipped: {failed}, Titles without year: {year_misses}, Listings without mileage: {mileage_misses}")

    except Exception as e:
        print(f"[ERROR] Database operation failed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
