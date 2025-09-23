import json
from bs4 import BeautifulSoup

# --- Load files ---
with open("makepage.html", "r", encoding="utf-8") as f:
    html = f.read()

with open("model_ids.json", "r", encoding="utf-8") as f:
    model_ids = json.load(f)  # should be a list like ["91054436", "12345678", ...]

# --- Parse HTML for all titles ---
soup = BeautifulSoup(html, "html.parser")
imgs = soup.find_all("img", class_="previous-listing-image")

titles = [img.get("alt", "").strip() for img in imgs]

# Check that counts match
if len(model_ids) != len(titles):
    raise ValueError(
        f"Mismatch: {len(model_ids)} IDs vs {len(titles)} titles. "
        "Ensure HTML and model_ids.json correspond."
    )

# --- Build dictionary ---
model_dict = {model_ids[i]: titles[i] for i in range(len(model_ids))}

# --- Save to JSON file ---
with open("models.json", "w", encoding="utf-8") as f:
    json.dump(model_dict, f, indent=2, ensure_ascii=False)

print(f"Saved {len(model_dict)} entries to models.json")
