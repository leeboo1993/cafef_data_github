#!/usr/bin/env python3
"""
VSD Trading Account Scraper - GitHub Actions Version
Scrapes stock trading account statistics from VSD
Default: Last 6 months to today
"""

import asyncio
import csv
import os
import re
import sys
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup


# ======================================================
# CONFIG
# ======================================================

BASE_URL = "https://www.vsd.vn"
START_URL = "https://www.vsd.vn/vi/alc/84"
CSV_FILE = "stock_trading_account/stock_trading_account.csv"

FIELDS = [
    "month",
    "domestic_account",
    "domestic_account_retail",
    "domestic_account_institution",
    "international_account",
    "international_account_retail",
    "international_account_institution",
]


# ======================================================
# HELPERS
# ======================================================

def parse_month(title: str) -> Optional[str]:
    title = title.replace("\xa0", " ")
    title = re.sub(r"\s+", " ", title)

    dates = re.findall(r"(\d{1,2})/(\d{1,2})/(\d{4})", title)
    if not dates:
        return None

    _, mm, yyyy = dates[-1]
    return f"{int(mm):02d}/{yyyy}"


def month_to_int(m: str) -> int:
    mm, yy = m.split("/")
    return int(yy) * 100 + int(mm)


def load_existing() -> Dict[str, Dict]:
    if not os.path.exists(CSV_FILE):
        return {}
    with open(CSV_FILE, encoding="utf-8") as f:
        return {r["month"]: r for r in csv.DictReader(f)}


def rewrite_all(rows: Dict[str, Dict]):
    from pathlib import Path
    # Create directory if it doesn't exist
    Path(CSV_FILE).parent.mkdir(parents=True, exist_ok=True)
    
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in sorted(rows.values(), key=lambda x: month_to_int(x["month"]), reverse=True):
            writer.writerow(r)


def validate_row(d: Dict) -> Tuple[bool, str]:
    try:
        if d["domestic_account_retail"] + d["domestic_account_institution"] != d["domestic_account"]:
            return False, "domestic sum mismatch"
        if d["international_account_retail"] + d["international_account_institution"] != d["international_account"]:
            return False, "foreign sum mismatch"
        return True, ""
    except Exception:
        return False, "missing fields"


# ======================================================
# MODERN TABLE PARSER (≥ 11/2020)
# ======================================================

async def modern_parser(page) -> Optional[Dict[str, int]]:
    """
    Parses modern VSD tables (November 2020+) with 2 tables structure.
    Returns dict or None.
    """
    try:
        # Wait for tables to load
        await page.wait_for_selector("table", timeout=5000)
    except:
        return None

    soup = BeautifulSoup(await page.content(), "html.parser")
    tables = soup.find_all("table")

    if len(tables) < 2:
        return None

    def parse_table(t):
        rows = t.find_all("tr")
        data = {}
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            key = cells[0].get_text(strip=True).lower()
            val_text = cells[1].get_text(strip=True)
            
            # Extract number
            num = re.sub(r"[^\d]", "", val_text)
            if num:
                data[key] = int(num)
        return data

    domestic = parse_table(tables[0])
    foreign = parse_table(tables[1])

    # Map keys
    def extract(d, *keys):
        for k in keys:
            for dk in d:
                if k in dk:
                    return d[dk]
        return None

    dom_total = extract(domestic, "tổng số", "tong so")
    dom_retail = extract(domestic, "cá nhân", "ca nhan")
    dom_inst = extract(domestic, "tổ chức", "to chuc")

    for_total = extract(foreign, "tổng số", "tong so")
    for_retail = extract(foreign, "cá nhân", "ca nhan")
    for_inst = extract(foreign, "tổ chức", "to chuc")

    if None in [dom_total, dom_retail, dom_inst, for_total, for_retail, for_inst]:
        return None

    return {
        "domestic_account": dom_total,
        "domestic_account_retail": dom_retail,
        "domestic_account_institution": dom_inst,
        "international_account": for_total,
        "international_account_retail": for_retail,
        "international_account_institution": for_inst,
    }


# ======================================================
# LEGACY PARSER (≤ 10/2020) — FINAL
# ======================================================

def legacy_parser(text: str) -> Optional[Dict[str, int]]:
    def grab(pattern: str) -> Optional[int]:
        m = re.search(pattern, text, re.I | re.S)
        if not m:
            return None
        return int(m.group(1).replace(".", "").replace(",", ""))

    values = []

    # --- extract bullet numbers ---
    for line in text.splitlines():
        line = line.strip()
        if not (line.startswith("-") or line.startswith("+")):
            continue

        nums = re.findall(r"\d[\d\.,]*", line)
        if not nums:
            continue

        try:
            values.append(int(nums[-1].replace(".", "").replace(",", "")))
        except ValueError:
            continue

    # ==================================================
    # CASE 1: perfect bullet structure (6 values)
    # ==================================================
    if len(values) == 6:
        return {
            "domestic_account": values[0],
            "domestic_account_retail": values[1],
            "domestic_account_institution": values[2],
            "international_account": values[3],
            "international_account_retail": values[4],
            "international_account_institution": values[5],
        }

    # ==================================================
    # CASE 2: missing BOTH totals (need regex totals)
    # ==================================================
    if len(values) == 4:
        domestic_total = grab(r"Số lượng TKGD trong nước\s*:\s*([\d\.,]+)")
        foreign_total = grab(r"Số lượng TKGD nước ngoài\s*:\s*([\d\.,]+)")

        if domestic_total and foreign_total:
            dom_retail, dom_inst, for_retail, for_inst = values
            if dom_retail + dom_inst == domestic_total and for_retail + for_inst == foreign_total:
                return {
                    "domestic_account": domestic_total,
                    "domestic_account_retail": dom_retail,
                    "domestic_account_institution": dom_inst,
                    "international_account": foreign_total,
                    "international_account_retail": for_retail,
                    "international_account_institution": for_inst,
                }

    # ==================================================
    # CASE 3: missing DOMESTIC total only (05/2019, 03/2020)
    # ==================================================
    if len(values) == 5:
        domestic_total = grab(r"Số lượng TKGD trong nước\s*:\s*([\d\.,]+)")
        if domestic_total:
            dom_retail, dom_inst, foreign_total, for_retail, for_inst = values
            if (
                dom_retail + dom_inst == domestic_total
                and for_retail + for_inst == foreign_total
            ):
                return {
                    "domestic_account": domestic_total,
                    "domestic_account_retail": dom_retail,
                    "domestic_account_institution": dom_inst,
                    "international_account": foreign_total,
                    "international_account_retail": for_retail,
                    "international_account_institution": for_inst,
                }

    # ==================================================
    # CASE 4: fully textual legacy (no bullets)
    # ==================================================
    domestic = grab(r"Số lượng TKGD trong nước.*?([\d\.,]+)\s*tài khoản")
    dom_retail = grab(r"trong nước.*?cá nhân.*?([\d\.,]+)\s*tài khoản")
    dom_inst = grab(r"trong nước.*?tổ chức.*?([\d\.,]+)\s*tài khoản")

    foreign = grab(r"Số lượng TKGD nước ngoài.*?([\d\.,]+)\s*tài khoản")
    for_retail = grab(r"nước ngoài.*?cá nhân.*?([\d\.,]+)\s*tài khoản")
    for_inst = grab(r"nước ngoài.*?tổ chức.*?([\d\.,]+)\s*tài khoản")

    if None not in (domestic, dom_retail, dom_inst, foreign, for_retail, for_inst):
        return {
            "domestic_account": domestic,
            "domestic_account_retail": dom_retail,
            "domestic_account_institution": dom_inst,
            "international_account": foreign,
            "international_account_retail": for_retail,
            "international_account_institution": for_inst,
        }

    return None


# ======================================================
# DETAIL ROUTER
# ======================================================

async def parse_detail(page, url: str, month: str) -> Optional[Dict]:
    await page.goto(url, timeout=60000)

    modern = await modern_parser(page)
    if modern:
        ok, reason = validate_row(modern)
        if ok:
            print(f"[OK] {month} modern parsed")
            return {"month": month, **modern}
        print(f"[INVALID] {month} modern → {reason}")

    soup = BeautifulSoup(await page.content(), "html.parser")
    legacy = legacy_parser(soup.get_text("\n"))

    if legacy:
        ok, reason = validate_row(legacy)
        if ok:
            print(f"[OK] {month} legacy parsed")
            return {"month": month, **legacy}
        print(f"[INVALID] {month} legacy → {reason}")

    print(f"[SKIP] {month} → no valid structure")
    return None


# ======================================================
# MAIN SCRAPER
# ======================================================

async def scrape_vsd_accounts(start_date=None, end_date=None, headless=True):
    start_i = month_to_int(start_date) if start_date else None
    end_i = month_to_int(end_date) if end_date else None

    # Try to download from R2 first
    try:
        if all([os.getenv("R2_ENDPOINT"), os.getenv("R2_ACCESS_KEY_ID"), 
                os.getenv("R2_SECRET_ACCESS_KEY"), os.getenv("R2_BUCKET")]):
            from utils_r2 import download_from_r2
            bucket = os.getenv("R2_BUCKET")
            r2_key = f"cafef_data/stock_trading_account/stock_trading_account.csv"
            
            # Create directory if it doesn't exist
            from pathlib import Path
            Path(CSV_FILE).parent.mkdir(parents=True, exist_ok=True)
            
            if download_from_r2(bucket, r2_key, CSV_FILE):
                 print(f"✅ Downloaded existing data from R2: {r2_key}")
    except Exception as e:
        print(f"⚠️ R2 download failed: {e}")

    existing = load_existing()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        ctx = await browser.new_context()
        list_page = await ctx.new_page()
        detail_page = await ctx.new_page()

        await list_page.goto(START_URL, timeout=60000)
        await list_page.wait_for_selector("ul.list-news")

        prev_sig = None
        repeat = 0
        current_page = 1

        while True:
            print(f"\n[PAGE] Crawling page {current_page}")

            cards = list_page.locator("ul.list-news > li")
            n = await cards.count()
            if n == 0:
                break

            sig = [await cards.nth(i).locator("h3 a").get_attribute("href") for i in range(n)]
            if sig == prev_sig:
                repeat += 1
                if repeat >= 2:
                    break
            else:
                repeat = 0
            prev_sig = sig

            for i in range(n):
                title = await cards.nth(i).locator("h3 a").inner_text()
                href = await cards.nth(i).locator("h3 a").get_attribute("href")

                month = parse_month(title)
                if not month:
                    continue

                mi = month_to_int(month)
                if end_i and mi > end_i:
                    continue
                if start_i and mi < start_i:
                    rewrite_all(existing)
                    await browser.close()
                    return

                if month in existing:
                    continue

                row = await parse_detail(detail_page, BASE_URL + href, month)
                if row:
                    existing[month] = row
                    rewrite_all(existing)

            current_page += 1
            await list_page.evaluate("(p) => changePage(p)", current_page)
            await list_page.wait_for_timeout(1200)

        rewrite_all(existing)
        await browser.close()
        print("[DONE] Crawl finished")

    # Upload to R2 if credentials are available
    try:
        import os
        if all([os.getenv("R2_ENDPOINT"), os.getenv("R2_ACCESS_KEY_ID"), 
                os.getenv("R2_SECRET_ACCESS_KEY"), os.getenv("R2_BUCKET")]):
            from utils_r2 import upload_to_r2
            bucket = os.getenv("R2_BUCKET")
            r2_key = f"cafef_data/stock_trading_account/stock_trading_account.csv"
            upload_to_r2(CSV_FILE, bucket, r2_key)
    except Exception as e:
        print(f"⚠️ R2 upload skipped: {e}")


# ======================================================
# CLI
# ======================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="VSD Trading Account scraper")
    parser.add_argument("--start", default=None, help="Start month in MM/YYYY format")
    parser.add_argument("--end", default=None, help="End month in MM/YYYY format")
    parser.add_argument("--show", action="store_true")

    args = parser.parse_args()

    # Default to last 6 months if no args
    if args.start is None and args.end is None:
        today = datetime.today()
        start_month = today - timedelta(days=180)
        args.start = start_month.strftime("%m/%Y")
        args.end = today.strftime("%m/%Y")
        print(f"🚀 GitHub Actions mode: {args.start} to {args.end}")

    asyncio.run(
        scrape_vsd_accounts(
            start_date=args.start,
            end_date=args.end,
            headless=not args.show,
        )
    )
