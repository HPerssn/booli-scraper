import sqlite3

def init_sqlite_db(db_path="listings.db"):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            price INTEGER,
            price_per_sqm INTEGER,
            rooms INTEGER,
            floor INTEGER,
            sqm INTEGER,
            date TEXT,
            lot_sqm INTEGER,
            housing_type TEXT,
            monthly_fee INTEGER,
            build_year INTEGER,
            starting_price INTEGER
        )
    ''')
    conn.commit()
    return conn

def insert_listing_to_db(conn, listing_dict):
    c = conn.cursor()
    c.execute('''
        INSERT INTO listings (
            address, price, price_per_sqm, rooms, floor, sqm, date, lot_sqm, housing_type, monthly_fee, build_year, starting_price
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        listing_dict.get("address"),
        listing_dict.get("price"),
        listing_dict.get("price_per_sqm"),
        listing_dict.get("rooms"),
        listing_dict.get("floor"),
        listing_dict.get("sqm"),
        listing_dict.get("date"),
        listing_dict.get("lot_sqm"),
        listing_dict.get("housing_type"),
        listing_dict.get("monthly_fee"),
        listing_dict.get("build_year"),
        listing_dict.get("starting_price")
    ))
    conn.commit()

def batch_insert_listings(conn, listings):
    c = conn.cursor()
    c.executemany('''
        INSERT INTO listings (
            address, price, price_per_sqm, rooms, floor, sqm, date, lot_sqm, housing_type, monthly_fee, build_year, starting_price
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
        (
            l.get("address"), l.get("price"), l.get("price_per_sqm"), l.get("rooms"),
            l.get("floor"), l.get("sqm"), l.get("date"), l.get("lot_sqm"),
            l.get("housing_type"), l.get("monthly_fee"), l.get("build_year"), l.get("starting_price")
        ) for l in listings
    ])
    conn.commit()

def export_listings_to_sqlite(listings, db_path="listings.db"):
    conn = init_sqlite_db(db_path)
    batch_insert_listings(conn, listings)
    conn.close()
    print(f"Saved {len(listings)} listings to SQLite database: {db_path}")
