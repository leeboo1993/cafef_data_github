#!/usr/bin/env python3
"""
VCB FX Data Scraper - GitHub Actions Version
Scrapes VietcomBank FX rates and saves to CSV
Default: T-5 to today
"""

import asyncio
import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
from playwright.async_api import async_playwright

VCB_URL = "https://www.vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"


# ======================================================
# Helpers
# ======================================================
def to_date(d):
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d").date()
    return d


def clean_num(x: str):
    x = x.strip()
    if x in ("", "-", None):
        return ""
    return x.replace(",", "")


def load_existing_dates(csv_file: Path) -> set:
    if not csv_file.exists():
        return set()

    out = set()
    with csv_file.open("r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row:
                out.add(row[0])
    return out


# ======================================================
# scrape 1 day
# ======================================================
async def scrape_one_day(page, d) -> List[List[str]]:
    ui_date = d.strftime("%d/%m/%Y")   # UI format
    csv_date = d.strftime("%Y-%m-%d")  # CSV format

    await page.fill("#datePicker", "")
    await page.fill("#datePicker", ui_date)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(1500)

    rows = page.locator("table.table-responsive tbody tr")
    count = await rows.count()

    results = []
    for i in range(count):
        r = rows.nth(i)
        tds = r.locator("td")

        if await tds.count() < 5:
            continue

        first = await tds.nth(0).inner_text()
        ticker = first.split()[-1].strip()

        name = (await tds.nth(1).inner_text()).strip()
        cash_buy = clean_num(await tds.nth(2).inner_text())
        deposit_buy = clean_num(await tds.nth(3).inner_text())
        sell = clean_num(await tds.nth(4).inner_text())

        results.append([csv_date, ticker, name, cash_buy, deposit_buy, sell])

    return results


# ======================================================
# Core async scraper — FIXED (USE FIREFOX)
# ======================================================
async def _scrape_vcb_fx(
    start_date: str,
    end_date: str,
    out_csv: str,
    update: bool,
    headless: bool,
):

    start = to_date(start_date)
    end = to_date(end_date)

    out_path = Path(out_csv)
    first_write = not out_path.exists()
    existing_dates = load_existing_dates(out_path)

    print(f"Loaded existing dates: {len(existing_dates)}")

    async with async_playwright() as p:

        # Use Chromium for better CI compatibility
        browser = await p.chromium.launch(headless=headless)

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="vi-VN",
            viewport={"width": 1400, "height": 900},
        )

        page = await context.new_page()

        # --- Retry logic (VCB sometimes resets HTTP2) ---
        for attempt in range(5):
            try:
                print(f"→ Loading page attempt {attempt+1}/5")
                await page.goto(VCB_URL, wait_until="domcontentloaded", timeout=90000)
                break
            except Exception as e:
                print(f"Retry goto… {attempt+1}/5: {e}")
                await asyncio.sleep(2)
                if attempt == 4:
                    raise

        await page.wait_for_timeout(2000)

        # Cookie popup
        try:
            btn = page.locator("button:has-text('Chấp nhận')")
            if await btn.count() > 0:
                await btn.first.click()
        except:
            pass

        # Write header
        if first_write:
            with out_path.open("w", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerow(
                    ["date", "ticker", "name", "cash_buy", "deposit_buy", "sell"]
                )

        # Loop by date
        d = start
        while d <= end:
            d_str = d.strftime("%Y-%m-%d")

            if not update and d_str in existing_dates:
                print(f"[SKIP] {d_str} exists")
                d += timedelta(days=1)
                continue

            print(f"[SCRAPE] {d_str}")
            rows = await scrape_one_day(page, d)

            if update:
                # Remove old rows for that day
                kept = []
                header = None

                with out_path.open("r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    for row in reader:
                        if row[0] != d_str:
                            kept.append(row)

                with out_path.open("w", newline="", encoding="utf-8-sig") as f:
                    w = csv.writer(f)
                    w.writerow(header)
                    for r in kept:
                        w.writerow(r)
                    for r in rows:
                        w.writerow(r)

            else:
                # Append mode
                with out_path.open("a", newline="", encoding="utf-8-sig") as f:
                    w = csv.writer(f)
                    for r in rows:
                        w.writerow(r)

            d += timedelta(days=1)

        await browser.close()

    print("DONE.")


# ======================================================
# Wrapper
# ======================================================
def scrape_vcb_fx(
    start_date: str = None,
    end_date: str = None,
    out_csv: str = "vcb_fx_data/vcb_fx_data.csv",
    update: bool = False,
    headless: bool = True,
):

    DEFAULT_START = "2020-02-01"

    # Case 1 — no dates → full history
    if start_date is None and end_date is None:
        start_date = DEFAULT_START
        end_date = datetime.today().strftime("%Y-%m-%d")
        update = False
        headless = True
        print(f"⚙ Default ⇒ {start_date} → {end_date}")

    # Case 2 — ONLY start_date → start_date → today
    elif start_date is not None and end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
        print(f"📅 Start date only ⇒ {start_date} → today ({end_date})")

    # Case 3 — ONLY end_date → default_start → end_date
    elif start_date is None and end_date is not None:
        start_date = DEFAULT_START
        print(f"📅 End date only ⇒ {DEFAULT_START} → {end_date}")

    # Case 4 — both provided → use them directly
    else:
        print(f"📅 Range ⇒ {start_date} → {end_date}")

    # Jupyter detection
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            print("Jupyter async detected → create_task")
            return loop.create_task(
                _scrape_vcb_fx(start_date, end_date, out_csv, update, headless)
            )
    except RuntimeError:
        pass

    # Normal mode
    print("Normal Python → asyncio.run()")
    asyncio.run(
        _scrape_vcb_fx(start_date, end_date, out_csv, update, headless)
    )
    
    # Upload to R2 if credentials are available
    try:
        import os
        if all([os.getenv("R2_ENDPOINT"), os.getenv("R2_ACCESS_KEY_ID"), 
                os.getenv("R2_SECRET_ACCESS_KEY"), os.getenv("R2_BUCKET")]):
            from utils_r2 import upload_to_r2
            bucket = os.getenv("R2_BUCKET")
            r2_key = f"cafef_data/vcb_fx_data/vcb_fx_data.csv"
            upload_to_r2(out_csv, bucket, r2_key)
    except Exception as e:
        print(f"⚠️ R2 upload skipped: {e}")


# ======================================================
# CLI
# ======================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="VCB FX scraper")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--out", default="vcb_fx_data/vcb_fx_data.csv")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--show", action="store_true")

    args = parser.parse_args()

    # Default to T-5 if no args
    if args.start is None and args.end is None:
        args.start = (datetime.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        args.update = True
        print(f"🚀 GitHub Actions mode: T-5 to today")

    scrape_vcb_fx(
        start_date=args.start,
        end_date=args.end,
        out_csv=args.out,
        update=args.update,
        headless=not args.show,
    )
