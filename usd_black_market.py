#!/usr/bin/env python3
"""
USD Black Market Scraper - GitHub Actions Version (Playwright Edition)
Scrapes USD/VND black market rates from tygiausd.org
Default: T-90 to today (Backfill enabled)
"""

import csv
import os
import datetime
import re
import unicodedata
import sys
import time
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

import urllib3
urllib3.disable_warnings()

CSV_PATH = "usd_black_market/usd_market_data.csv"
EARLIEST_DATE = datetime.date(2014, 7, 9)
PAGE_TIMEOUT_MS = 60000 

# ======================================================
# CLEAN NUMBER
# ======================================================
def clean_number(text: str):
    if not text:
        return None
    match = re.search(r"\d[\d,]*", text)
    if not match:
        return None
    try:
        return int(match.group(0).replace(",", ""))
    except:
        return None


# ======================================================
# PARSING LOGIC (BEAUTIFULSOUP)
# ======================================================
def normalize(text):
    text = text.lower().strip()
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    return text

def parse_black_market(soup, date_str):
    """Extract Black Market data from soup"""
    try:
        # Verify date matches if possible? Site updates input#date
        date_input = soup.find("input", {"id": "date"})
        date_val = date_input.get("value") if date_input else date_str
        
        # Look for "USD tự do"
        # Sometimes it might be slightly different text, but usually "USD tự do"
        row = None
        strong = soup.find("strong", string=re.compile(r"USD tự do", re.I))
        if strong:
            row = strong.find_parent("tr")
        
        if not row:
            return None

        tds = row.find_all("td")
        if len(tds) < 2:
            return None
            
        buy = clean_number(tds[0].get_text(" ", strip=True))
        sell = clean_number(tds[1].get_text(" ", strip=True))

        if buy is None or sell is None:
            return None

        return {
            "date": date_val,
            "usd_buy_black_market": buy,
            "usd_sell_black_market": sell,
            "gap_black_market": sell - buy,
        }
    except Exception as e:
        print(f"⚠️ Error parsing black market for {date_str}: {e}")
        return None

def parse_bank_usd(soup, date_str):
    """Extract Bank USD data from soup"""
    try:
        usd_tag = soup.find(
            lambda tag: tag.name in ["strong", "a"] and tag.get_text(strip=True) == "USD"
        )
        if not usd_tag:
            return None

        row = usd_tag.find_parent("tr")
        table = row.find_parent("table")
        if not table:
            return None

        # Find header row
        header_tr = None
        for tr in table.find_all("tr"):
            raw = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if any("Mã NT" in h for h in raw) or any("Mua vào" in h for h in raw):
                header_tr = tr
                break

        if not header_tr:
            return None

        headers = [
            normalize(c.get_text(" ", strip=True))
            for c in header_tr.find_all(["th", "td"])
        ]

        buy_idx = sell_idx = deposit_idx = None
        for idx, h in enumerate(headers):
            if "mua" in h:
                buy_idx = idx
            elif "ban" in h:
                sell_idx = idx
            elif "chuyen" in h:
                deposit_idx = idx

        if buy_idx is None or sell_idx is None:
            return None

        cells = row.find_all(["td", "th"])
        # Ensure indices exist
        if max(buy_idx, sell_idx) >= len(cells):
            return None
            
        buy = clean_number(cells[buy_idx].get_text(" ", strip=True))
        sell = clean_number(cells[sell_idx].get_text(" ", strip=True))
        
        deposit = None
        if deposit_idx is not None and deposit_idx < len(cells):
            deposit = clean_number(cells[deposit_idx].get_text(" ", strip=True))

        if buy is None or sell is None:
            return None

        return {
            "usd_buy_bank": buy,
            "usd_sell_bank": sell,
            "usd_deposit_bank": deposit,
            "gap_bank": sell - buy,
        }
    except Exception as e:
        print(f"⚠️ Error parsing bank USD for {date_str}: {e}")
        return None

# ======================================================
# CSV HELPERS
# ======================================================
FIELDNAMES = [
    "date",
    "usd_buy_black_market",
    "usd_sell_black_market",
    "usd_buy_bank",
    "usd_sell_bank",
    "usd_deposit_bank",
    "gap_black_market",
    "gap_bank",
    "difference_buy",
    "difference_sell",
]

def read_csv_rows(csv_path=CSV_PATH):
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(rows, csv_path=CSV_PATH):
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def append_to_csv(row, csv_path=CSV_PATH):
    from pathlib import Path
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not exists:
            writer.writeheader()
        writer.writerow(row)

# ======================================================
# DETECT MISSING ROWS
# ======================================================
CRITICAL_FIELDS = [
    "usd_buy_black_market",
    "usd_sell_black_market",
    "usd_buy_bank",
    "usd_sell_bank",
]

def detect_missing_or_incomplete_rows(csv_path=CSV_PATH):
    if not os.path.exists(csv_path):
        return set(), []

    rows = read_csv_rows(csv_path)
    existing_dates = set()
    missing_or_incomplete = []

    for row in rows:
        existing_dates.add(row["date"])
        for f in CRITICAL_FIELDS:
            v = row.get(f)
            if v in [None, "", "None"]:
                missing_or_incomplete.append(row["date"])
                break

    return existing_dates, missing_or_incomplete

# ======================================================
# MAIN SCRAPER
# ======================================================
def usd_vnd_black_market(
    start_date=None,
    end_date=None,
    csv_path=CSV_PATH,
    update=False,
):
    # ------------------------------------------------
    # 1. R2 DOWNLOAD
    # ------------------------------------------------
    import os
    if update:
        try:
            if all([os.getenv("R2_ENDPOINT"), os.getenv("R2_ACCESS_KEY_ID"), 
                    os.getenv("R2_SECRET_ACCESS_KEY"), os.getenv("R2_BUCKET")]):
                from utils_r2 import download_from_r2, list_r2_files
                bucket = os.getenv("R2_BUCKET")
                
                files = list_r2_files(bucket, "cafef_data/usd_black_market/usd_market_data_")
                if not files:
                     files = list_r2_files(bucket, "cafef_data/usd_black_market/usd_market_data.csv")
                
                if files:
                    r2_key = sorted(files)[-1]
                else:
                    r2_key = "cafef_data/usd_black_market/usd_market_data.csv"
                
                from pathlib import Path
                Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
                
                if download_from_r2(bucket, r2_key, csv_path):
                    print(f"✅ Downloaded existing data from R2: {r2_key}")
                else:
                    print(f"ℹ️ No existing data found on R2: {r2_key}")
        except Exception as e:
            print(f"⚠️ R2 download failed: {e}")

    # ------------------------------------------------
    # 2. DETERMINE TARGET DATES
    # ------------------------------------------------
    if start_date is None:
        start = EARLIEST_DATE
    else:
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        start = max(start, EARLIEST_DATE)

    if end_date is None:
        end = datetime.date.today()
    else:
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    delta = datetime.timedelta(days=1)
    target_dates = []

    if update:
        print("🔍 Smart update mode (range-limited) enabled")
        existing_dates, incomplete_dates = detect_missing_or_incomplete_rows(csv_path)
        cur = start
        while cur <= end:
            d = cur.strftime("%d-%m-%Y")
            if d not in existing_dates or d in incomplete_dates:
                target_dates.append(d)
                # Keep looping to find all missing
            
            # Special case: If today is incorrectly marked as existing but we want to refresh?
            # Trust user/detect logic.
            
            cur += delta
    else:
        print("Normal full-range crawl mode")
        # Logic: If not update, we might overwrite or append? 
        # Typically scripts here use update=True for GitHub Actions.
        # If update=False, simple loop. 
        # CAUTION: current append implementation might duplicate if not checking existing.
        # Let's enforce checking existing to prevent duplicates even in non-update mode if scraping
        # is just appended. Ideally, we build a fresh list or map.
        # For safety and speed, we will use a "smart append":
        existing_dates_simple = set(row["date"] for row in read_csv_rows(csv_path))
        cur = start
        while cur <= end:
            d = cur.strftime("%d-%m-%Y")
            if d not in existing_dates_simple:
                target_dates.append(d)
            cur += delta
            
    # Sort dates
    target_dates = sorted(
        list(set(target_dates)),
        key=lambda d: datetime.datetime.strptime(d, "%d-%m-%Y"),
    )

    print(f"📌 Total dates to crawl: {len(target_dates)}")
    if not target_dates:
        print("✅ Nothing to update.")
        return

    # ------------------------------------------------
    # 3. PLAYWRIGHT SCRAPING LOOP
    # ------------------------------------------------
    processed_rows = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Use context with custom user agent and viewport to be more "human"
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()
        
        for date_str in target_dates:
            print(f"Scraping {date_str}...")
            url = f"https://tygiausd.org/TyGia?date={date_str}"
            
            try:
                # Go to page
                page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
                
                # Wait for key element (the date input or table)
                try:
                    page.wait_for_selector("input#date", timeout=5000)
                except:
                    # Maybe it's not loaded or different structure, proceed to parse anyway
                    pass
                
                # Get extraction
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                black = parse_black_market(soup, date_str)
                bank = parse_bank_usd(soup, date_str)
                
                if not black:
                    print("  → No black market data")
                    # Could fail due to blocking or actual no data.
                    # Retry logic? Playwright is usually robust.
                    continue
                
                if not bank:
                    print("  → No bank data")
                    bank = {
                        "usd_buy_bank": None,
                        "usd_sell_bank": None,
                        "usd_deposit_bank": None,
                        "gap_bank": None,
                    }

                # Calculate diffs
                if bank["usd_buy_bank"] and bank["usd_sell_bank"]:
                    diff_buy = black["usd_buy_black_market"] / bank["usd_buy_bank"] - 1
                    diff_sell = black["usd_sell_black_market"] / bank["usd_sell_bank"] - 1
                else:
                    diff_buy = None
                    diff_sell = None

                row = {
                    "date": black["date"], # Use the date from the page input
                    "usd_buy_black_market": black["usd_buy_black_market"],
                    "usd_sell_black_market": black["usd_sell_black_market"],
                    "usd_buy_bank": bank["usd_buy_bank"],
                    "usd_sell_bank": bank["usd_sell_bank"],
                    "usd_deposit_bank": bank["usd_deposit_bank"],
                    "gap_black_market": black["gap_black_market"],
                    "gap_bank": bank["gap_bank"],
                    "difference_buy": diff_buy,
                    "difference_sell": diff_sell,
                }
                
                processed_rows.append(row)
                print("  → Captured")
                
                # Nice polite delay
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ Failed to scrape {date_str}: {e}")
                
        browser.close()
    
    # ------------------------------------------------
    # 4. SAVE TO CSV
    # ------------------------------------------------
    # We need to merge with existing data intelligently
    # Read all existing
    all_rows_map = {r["date"]: r for r in read_csv_rows(csv_path)}
    
    # Update with new
    for row in processed_rows:
        all_rows_map[row["date"]] = row
        
    # Sort
    sorted_rows = sorted(
        all_rows_map.values(),
        key=lambda r: datetime.datetime.strptime(r["date"], "%d-%m-%Y") if "-" in r["date"] else datetime.datetime.min # Handle potential bad dates?
    )
    
    # Write back
    write_csv_rows(sorted_rows, csv_path)
    print(f"✅ CSV updated with {len(processed_rows)} new rows.")

    # ------------------------------------------------
    # 5. R2 UPLOAD
    # ------------------------------------------------
    try:
        import os
        if all([os.getenv("R2_ENDPOINT"), os.getenv("R2_ACCESS_KEY_ID"), 
                os.getenv("R2_SECRET_ACCESS_KEY"), os.getenv("R2_BUCKET")]):
            from utils_r2 import upload_to_r2, backup_and_cleanup_r2
            bucket = os.getenv("R2_BUCKET")
            
            # Determine date suffix
            max_date_str = ""
            if sorted_rows:
                max_date_str = sorted_rows[-1]["date"]
            
            if max_date_str:
                try:
                    dt_obj = datetime.datetime.strptime(max_date_str, "%d-%m-%Y")
                except:
                    dt_obj = datetime.datetime.now()
                date_suffix = dt_obj.strftime("%y%m%d")
            else:
                date_suffix = datetime.datetime.now().strftime("%y%m%d")
                
            r2_key = f"cafef_data/usd_black_market/usd_market_data_{date_suffix}.csv"

            upload_to_r2(csv_path, bucket, r2_key)
            print(f"☁️ Uploaded to R2: {r2_key}")
            
            print("🧹 Cleaning old backups for usd_black_market in R2...")
            backup_and_cleanup_r2(bucket, "cafef_data/usd_black_market/", keep=1)
    except Exception as e:
        print(f"⚠️ R2 upload/cleanup error: {e}")


# ======================================================
# CLI
# ======================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="USD Black Market scraper (Playwright)")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--csv", default=CSV_PATH)
    parser.add_argument("--update", action="store_true")

    args = parser.parse_args()

    # Default to T-90 if no args (Fill gaps, smart update skips existing)
    if args.start is None and args.end is None:
        args.start = (datetime.date.today() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
        args.update = True
        print(f"🚀 GitHub Actions mode: T-90 to today (Backfill enabled)")

    usd_vnd_black_market(
        start_date=args.start,
        end_date=args.end,
        csv_path=args.csv,
        update=args.update,
    )
