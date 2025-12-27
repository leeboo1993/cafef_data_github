#!/usr/bin/env python3
"""
USD Black Market Scraper - GitHub Actions Version
Scrapes USD/VND black market rates from tygiausd.org
Default: T-5 to today
"""

import requests
import csv
import os
import datetime
import re
import unicodedata
import sys
from bs4 import BeautifulSoup


CSV_PATH = "usd_black_market/usd_market_data.csv"
EARLIEST_DATE = datetime.date(2014, 7, 9)


# ======================================================
# CLEAN NUMBER
# ======================================================
def clean_number(text: str):
    if not text:
        return None
    match = re.search(r"\d[\d,]*", text)
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


# ======================================================
# BLACK MARKET USD ("USD tự do")
# ======================================================
def scrape_black_market(date_str: str):
    url = f"https://tygiausd.org/TyGia?date={date_str}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")

    date_input = soup.find("input", {"id": "date"})
    if not date_input:
        return None

    date_val = date_input.get("value")

    try:
        row = soup.find("strong", string="USD tự do").find_parent("tr")
        tds = row.find_all("td")
        buy = clean_number(tds[0].get_text(" ", strip=True))
        sell = clean_number(tds[1].get_text(" ", strip=True))
    except Exception:
        return None

    if buy is None or sell is None:
        return None

    return {
        "date": date_val,
        "usd_buy_black_market": buy,
        "usd_sell_black_market": sell,
        "gap_black_market": sell - buy,
    }


# ======================================================
# BANK USD SCRAPER
# ======================================================
def normalize(text):
    text = text.lower().strip()
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    return text


def scrape_bank_usd(date_str: str):
    url = f"https://tygiausd.org/TyGia?date={date_str}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")

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
    buy = clean_number(cells[buy_idx].get_text(" ", strip=True))
    sell = clean_number(cells[sell_idx].get_text(" ", strip=True))
    deposit = clean_number(cells[deposit_idx].get_text(" ", strip=True)) if deposit_idx is not None else None

    if buy is None or sell is None:
        return None

    return {
        "usd_buy_bank": buy,
        "usd_sell_bank": sell,
        "usd_deposit_bank": deposit,
        "gap_bank": sell - buy,
    }


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
    "usd_deposit_bank",
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
# MAIN SCRAPER (RANGE-LIMITED UPDATE MODE)
# ======================================================
def usd_vnd_black_market(
    start_date=None,
    end_date=None,
    csv_path=CSV_PATH,
    update=False,
):
    # Parse input dates
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

    # ======================================================
    # SMART UPDATE MODE → ONLY WITHIN RANGE
    # ======================================================
    if update:
        print("🔍 Smart update mode (range-limited) enabled")

        existing_dates, incomplete_dates = detect_missing_or_incomplete_rows(csv_path)
        existing_rows = read_csv_rows(csv_path)
        by_date = {row["date"]: row for row in existing_rows}

        target_dates = []
        cur = start
        while cur <= end:
            d = cur.strftime("%d-%m-%Y")

            if d not in existing_dates or d in incomplete_dates:
                target_dates.append(d)

            cur += delta

        target_dates = sorted(
            list(set(target_dates)),
            key=lambda d: datetime.datetime.strptime(d, "%d-%m-%Y"),
        )

        print(f"📌 Total dates to crawl within range: {len(target_dates)}")
        print(target_dates)

        for date_str in target_dates:
            print(f"Scraping {date_str}...")

            black = scrape_black_market(date_str)
            bank = scrape_bank_usd(date_str)

            if not black:
                print("  → No black market data")
                continue

            if not bank:
                print("  → No bank data")
                bank = {
                    "usd_buy_bank": None,
                    "usd_sell_bank": None,
                    "usd_deposit_bank": None,
                    "gap_bank": None,
                }

            # premium
            if bank["usd_buy_bank"] and bank["usd_sell_bank"]:
                diff_buy = black["usd_buy_black_market"] / bank["usd_buy_bank"] - 1
                diff_sell = black["usd_sell_black_market"] / bank["usd_sell_bank"] - 1
            else:
                diff_buy = None
                diff_sell = None

            row = {
                "date": black["date"],
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

            by_date[row["date"]] = row

        all_dates = sorted(
            by_date.keys(),
            key=lambda d: datetime.datetime.strptime(d, "%d-%m-%Y"),
        )

        write_csv_rows([by_date[d] for d in all_dates], csv_path)
        print("✅ CSV updated")
        return

    # ======================================================
    # NORMAL MODE → FULL RANGE
    # ======================================================
    print("Normal full-range crawl mode")
    existing_dates = set(row["date"] for row in read_csv_rows(csv_path))

    cur = start
    while cur <= end:
        date_str = cur.strftime("%d-%m-%Y")

        if date_str in existing_dates:
            print(f"Skip {date_str} (exists)")
            cur += delta
            continue

        print(f"Scraping {date_str}...")

        black = scrape_black_market(date_str)
        bank = scrape_bank_usd(date_str)

        if not black:
            print("  → No black market data")
            cur += delta
            continue

        if not bank:
            print("  → No bank data")
            bank = {
                "usd_buy_bank": None,
                "usd_sell_bank": None,
                "usd_deposit_bank": None,
                "gap_bank": None,
            }

        if bank["usd_buy_bank"] and bank["usd_sell_bank"]:
            diff_buy = black["usd_buy_black_market"] / bank["usd_buy_bank"] - 1
            diff_sell = black["usd_sell_black_market"] / bank["usd_sell_bank"] - 1
        else:
            diff_buy = None
            diff_sell = None

        row = {
            "date": black["date"],
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

        append_to_csv(row, csv_path)
        print("  → Saved")
        cur += delta

    # Upload to R2 if credentials are available
    try:
        import os
        if all([os.getenv("R2_ENDPOINT"), os.getenv("R2_ACCESS_KEY_ID"), 
                os.getenv("R2_SECRET_ACCESS_KEY"), os.getenv("R2_BUCKET")]):
            from utils_r2 import upload_to_r2
            bucket = os.getenv("R2_BUCKET")
            r2_key = f"cafef_data/usd_black_market/usd_market_data.csv"
            upload_to_r2(csv_path, bucket, r2_key)
    except Exception as e:
        print(f"⚠️ R2 upload skipped: {e}")


# ======================================================
# CLI
# ======================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="USD Black Market scraper")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--csv", default=CSV_PATH)
    parser.add_argument("--update", action="store_true")

    args = parser.parse_args()

    # Default to T-5 if no args
    if args.start is None and args.end is None:
        args.start = (datetime.date.today() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
        args.update = True
        print(f"🚀 GitHub Actions mode: T-5 to today")

    usd_vnd_black_market(
        start_date=args.start,
        end_date=args.end,
        csv_path=args.csv,
        update=args.update,
    )
