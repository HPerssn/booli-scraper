# estimate_pages.py
"""
Estimate how many pages you need to scrape to cover N years of Booli apartment sales data.
Assumes you know the number of listings per page and can provide an average number of listings per day.
"""

def estimate_pages(years=5, listings_per_day=140, listings_per_page=20):
    days = years * 365
    total_listings = days * listings_per_day
    pages = total_listings // listings_per_page
    if total_listings % listings_per_page:
        pages += 1
    print(f"For {years} years, with {listings_per_day} listings/day and {listings_per_page} listings/page:")
    print(f"Total listings: {total_listings}")
    print(f"Estimated pages needed: {pages}")
    return pages

if __name__ == "__main__":
    # You can adjust these numbers as needed
    estimate_pages(years=5, listings_per_day=140, listings_per_page=20)
    
