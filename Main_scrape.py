from dataclasses import dataclass, asdict, fields
import httpx
from selectolax.parser import HTMLParser
import time
import random
import datetime
import csv
import re
import json
from db_utils import export_listings_to_sqlite
import asyncio
import logging
import sys

today = datetime.date.today()

@dataclass
class Listing:
    address: str
    price: int | None
    price_per_sqm: int | None
    rooms: int | None
    floor: int | None
    sqm: int | None
    date: str | None
    lot_sqm: int | None = None
    housing_type: str | None = None
    monthly_fee: int | None = None
    build_year: int | None = None
    starting_price: int | None = None
    page: int | None = None  # Track which page this listing was scraped from


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

    sqm = rooms = floor = price_per_sqm = lot_sqm = None
    data_list = item.css_first(".object-card__data-list")
    if data_list:
        for li in data_list.css("li"):
            aria = li.attributes.get("aria-label", "")
            text = li.text().strip()
            if "kvadratmeter" in aria and "tomt" in aria:
                lot_sqm = clean_value(text, "kvm_tomt")
            elif "kvadratmeter" in aria and "kr" not in aria:
                sqm = clean_value(text, "kvm")
            elif "rum" in aria:
                rooms = clean_value(text, "rum")
            elif "vån" in aria:
                floor = clean_value(text, "van")
            elif "kr/kvadratmeter" in aria or "kr/m²" in text:
                price_per_sqm = clean_value(text, "pris_per_kvm")

    return Listing(address, price, price_per_sqm, rooms, floor, sqm, date, lot_sqm, housing_type)

def extract_booli_detail_data(html):
    # Extract the __NEXT_DATA__ JSON
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        return None, None, None
    data = json.loads(match.group(1))
    try:
        sold_property = data["props"]["pageProps"]["__APOLLO_STATE__"]
        sold_key = next(k for k in sold_property if k.startswith("SoldProperty:"))
        prop = sold_property[sold_key]
        monthly_fee = prop.get("rent", {}).get("raw")
        build_year = prop.get("constructionYear")
        starting_price = prop.get("listPrice", {}).get("raw")
        return monthly_fee, build_year, starting_price
    except Exception as e:
        print("Extraction error:", e)
        return None, None, None

def extract_detail_url(item):
    detail_node = item.css_first(".object-card__header a")
    if detail_node:
        href = detail_node.attributes["href"]
        if href.startswith("http"):
            return href
        return f"https://www.booli.se{href}"
    return None


async def fetch_with_retries(request_func, url, headers, client, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = await request_func(url, headers=headers, timeout=20)
            if resp.status_code == 202:
                wait = 2 + attempt * 2
                print(f"202 Accepted for {url}, retrying in {wait}s (attempt {attempt+1}/{max_retries})...")
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            print(f"Request failed for {url}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 + attempt * 2)
            else:
                return None

async def fetch_page(url, headers, client):
    return await fetch_with_retries(client.get, url, headers, client)

async def fetch_listing_detail_async(detail_url, headers, client):
    try:
        resp = await client.get(detail_url, headers=headers, timeout=20)
        resp.raise_for_status()
        return extract_booli_detail_data(resp.text)
    except Exception as e:
        print(f"Failed to fetch or parse detail page {detail_url}: {e}")
        return None, None, None

async def scrape_listings_async(location_id, max_pages=4, concurrency=5):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    }
    listings = []
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        def get_page_range(start_page, max_pages):
            return range(start_page, max_pages + 1)
        # Accept a start_page argument
        start_page = getattr(scrape_listings_async, 'start_page', 1)
        page_urls = [f"https://www.booli.se/sok/slutpriser?areaIds={location_id}&objectType=Lägenhet&page={page}" for page in get_page_range(start_page, max_pages)]
        logging.info(f"Starting to fetch pages {start_page} to {max_pages}...")
        page_results = []
        # Fetch each page with a random delay to avoid being blocked
        for url in page_urls:
            html = await fetch_page(url, headers, client)
            page_results.append(html)
            delay = random.uniform(0.3, 1.2)  #delay
            logging.info(f"Sleeping {delay:.2f}s before next page request...")
            await asyncio.sleep(delay)
        for idx, page_html in enumerate(page_results, start_page):
            if not page_html:
                continue
            html = HTMLParser(page_html)
            items = html.css("div ul li.search-page__module-container")
            if not items:
                continue
            detail_tasks = []
            for item in items:
                listing = extract_listing(item)
                if not listing.address:
                    continue
                listing_dict = asdict(listing)
                listing_dict["page"] = idx  # Track which page this listing came from
                detail_url = extract_detail_url(item)
                if detail_url:
                    async def fetch_and_update(listing_dict=listing_dict, detail_url=detail_url):
                        async with sem:
                            await asyncio.sleep(random.uniform(0.1, 1.0))
                            monthly_fee, build_year, starting_price = await fetch_listing_detail_async(detail_url, headers, client)
                            listing_dict["monthly_fee"] = monthly_fee
                            listing_dict["build_year"] = build_year
                            listing_dict["starting_price"] = starting_price
                            listings.append(listing_dict)
                    detail_tasks.append(fetch_and_update())
                else:
                    listings.append(listing_dict)
            # Batch detail tasks in groups of 5, with a pause between batches
            batch_size = 5
            for i in range(0, len(detail_tasks), batch_size):
                batch = detail_tasks[i:i+batch_size]
                await asyncio.gather(*batch)
                if i + batch_size < len(detail_tasks):
                    pause = random.uniform(5, 10)
                    logging.info(f"Batch pause: sleeping {pause:.2f}s before next batch of detail fetches...")
                    await asyncio.sleep(pause)
            if idx % 100 == 0:
                logging.info(f"Processed {idx} pages out of {max_pages}")
    logging.info(f"Total listings scraped: {len(listings)}")
    return listings

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    location = "Malmö"
    if location == "Malmö":
        location = 78

    start_page = 1
    if len(sys.argv) > 1:
        try:
            start_page = int(sys.argv[1])
        except Exception:
            
            logging.warning("Invalid start_page argument, defaulting to 1.")
    # Pass start_page to the async function via attribute
    setattr(scrape_listings_async, 'start_page', start_page)
    try:
        listings = asyncio.run(scrape_listings_async(location, max_pages=924, concurrency=3))
        export_listings_to_sqlite(listings)
        #export_listings_to_csv(listings)
    except KeyboardInterrupt:
        logging.warning("Scraper stopped by user.")
