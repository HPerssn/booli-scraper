from dataclasses import dataclass, asdict, fields
import httpx
from selectolax.parser import HTMLParser
import time
import random
import datetime
import csv
import re

today = datetime.date.today()

@dataclass
class Listing:
    address: str
    price: int | None
    pris_per_kvm: int | None
    rum: int | None
    van: int | None
    kvm: int | None
    date: str | None
    kvm_tomt: int | None = None
    housing_type: str | None = None


def clean_value(text, value_type=None):
    if not text:
        return None
    text = text.replace("\xa0", " ").strip()
    if value_type in ("price", "pris_per_kvm", "kvm_tomt", "kvm", "rum", "van"):
        match = re.search(r"\d+", text.replace(" ", ""))
        return int(match.group()) if match else None
    return text


def export_listings_to_csv(listings, filename=f"listings_{today}.csv"):
    field_names = [field.name for field in fields(Listing)]
    with open(filename, "w") as f:
        writer = csv.DictWriter(f, field_names)
        writer.writeheader()
        writer.writerows(listings)
    print("saved to csv:", filename)

def extract_listing(item):
    def get_text(selector):
        node = item.css_first(selector)
        return node.text().strip() if node else None

    address = clean_value(get_text(".object-card__header"))
    price = clean_value(get_text(".object-card__price__logo"), "price")
    date = clean_value(get_text(".object-card__date__logo"))
    preamble = get_text(".object-card__preamble")
    housing_type = preamble.split("·")[0].strip().capitalize() if preamble else None

    kvm = rum = van = pris_per_kvm = kvm_tomt = None
    data_list = item.css_first(".object-card__data-list")
    if data_list:
        for li in data_list.css("li"):
            aria = li.attributes.get("aria-label", "")
            text = li.text().strip()
            if "kvadratmeter" in aria and "tomt" in aria:
                kvm_tomt = clean_value(text, "kvm_tomt")
            elif "kvadratmeter" in aria and "kr" not in aria:
                kvm = clean_value(text, "kvm")
            elif "rum" in aria:
                rum = clean_value(text, "rum")
            elif "vån" in aria:
                van = clean_value(text, "van")
            elif "kr/kvadratmeter" in aria or "kr/m²" in text:
                pris_per_kvm = clean_value(text, "pris_per_kvm")

    return Listing(address, price, pris_per_kvm, rum, van, kvm, date, kvm_tomt, housing_type)

def scrape_listings(location_id, max_pages=4):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    }
    listings = []
    for page in range(1, max_pages + 1):
        url = f"https://www.booli.se/sok/slutpriser?areaIds={location_id}&objectType=Lägenhet&page={page}"
        try:
            resp = httpx.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"Request failed for page {page}: {e}")
            continue
        html = HTMLParser(resp.text)
        items = html.css("div ul li.search-page__module-container")
        if not items:
            break
        for item in items:
            listing = extract_listing(item)
            if listing.address:
                print(asdict(listing))
                listings.append(asdict(listing))
        time.sleep(random.uniform(1, 3))
    print(f"Named addresses: {len(listings)}")
    return listings

if __name__ == "__main__":
    location = "Malmö"
    if location == "Malmö":
        location = 78
    listings = scrape_listings(location, max_pages=4)
    export_listings_to_csv(listings)
