import sqlite3
import asyncio
import httpx
import logging
import random
from Main_scrape import extract_booli_detail_data

def get_incomplete_listings(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, address, date FROM listings
        WHERE monthly_fee IS NULL OR build_year IS NULL OR starting_price IS NULL
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

from selectolax.parser import HTMLParser

def build_search_url(address, date, area_id=78):
    # Format date as YYYY-MM-DD
    # You may need to adjust area_id for other locations
    return f"https://www.booli.se/sok/slutpriser?areaIds={area_id}&maxSoldDate={date}&minSoldDate={date}&objectType=L%C3%A4genhet"

def normalize_address(addr):
    # Lowercase, remove extra spaces, etc. for matching
    return addr.lower().replace(" ", "").replace(",", "")

async def fetch_and_update_listing(id, address, date, db_path):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    }
    search_url = build_search_url(address, date)
    async with httpx.AsyncClient() as client:
        try:
            # Retry logic for search page
            for attempt in range(5):
                resp = await client.get(search_url, headers=headers, timeout=20)
                if resp.status_code == 202:
                    wait = 2 + attempt * 2
                    logging.warning(f"202 Accepted for search {search_url}, retrying in {wait}s (attempt {attempt+1}/5)...")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            else:
                logging.error(f"Failed to get search page for {id} after retries.")
                return
            html = HTMLParser(resp.text)
            items = html.css("div ul li.search-page__module-container")
            found_url = None
            for item in items:
                addr = item.css_first(".object-card__header")
                if addr and normalize_address(addr.text()) == normalize_address(address):
                    detail_node = item.css_first(".object-card__header a")
                    if detail_node:
                        href = detail_node.attributes["href"]
                        found_url = href if href.startswith("http") else f"https://www.booli.se{href}"
                        break
            if not found_url:
                logging.warning(f"Could not find detail URL for listing {id} ({address}, {date})")
                return
            # Retry logic for detail page
            for attempt in range(5):
                detail_resp = await client.get(found_url, headers=headers, timeout=20)
                if detail_resp.status_code == 202:
                    wait = 2 + attempt * 2
                    logging.warning(f"202 Accepted for detail {found_url}, retrying in {wait}s (attempt {attempt+1}/5)...")
                    await asyncio.sleep(wait)
                    continue
                detail_resp.raise_for_status()
                break
            else:
                logging.error(f"Failed to get detail page for {id} after retries.")
                return
            monthly_fee, build_year, starting_price = extract_booli_detail_data(detail_resp.text)
            if monthly_fee or build_year or starting_price:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE listings SET monthly_fee=?, build_year=?, starting_price=? WHERE id=?
                """, (monthly_fee, build_year, starting_price, id))
                conn.commit()
                conn.close()
                logging.info(f"Updated listing {id}")
            else:
                logging.warning(f"No new data for listing {id}")
        except Exception as e:
            logging.error(f"Failed to fetch/update listing {id}: {e}")

async def retry_missing_details(db_path, batch_size=5):
    listings = get_incomplete_listings(db_path)
    logging.info(f"Found {len(listings)} incomplete listings.")
    for i in range(0, len(listings), batch_size):
        batch = listings[i:i+batch_size]
        tasks = [fetch_and_update_listing(id, address, date, db_path) for id, address, date in batch]
        await asyncio.gather(*tasks)
        pause = random.uniform(5, 10)
        logging.info(f"Batch pause: sleeping {pause:.2f}s before next batch...")
        await asyncio.sleep(pause)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_path = "listings.db"
    asyncio.run(retry_missing_details(db_path))
