import requests
from bs4 import BeautifulSoup

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

# Example
url = "https://bringatrailer.com/listing/2000-honda-civic-si-34/"  # replace with real page
print(extract_listing_details(url))
