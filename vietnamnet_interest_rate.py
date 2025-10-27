import asyncio
import re
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from utils_r2 import upload_to_r2, clean_old_backups_r2, ensure_folder_exists

# ============================================================
# CONFIG
# ============================================================
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path(os.getcwd()).resolve()

SAVE_DIR = BASE_DIR / "deposit_rate"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# HELPERS
# ============================================================
async def safe_goto(page, url, max_retries=3, timeout=60000):
    for attempt in range(1, max_retries + 1):
        try:
            await page.goto(url, timeout=timeout)
            await page.wait_for_load_state("domcontentloaded", timeout=15000)
            return True
        except Exception as e:
            print(f"⚠️ Navigation attempt {attempt}/{max_retries} failed: {e}")
            await asyncio.sleep(2)
    return False


def parse_interest_table(html, fallback_date):
    """Extract deposit rate table into a clean dataframe."""
    soup = BeautifulSoup(html, "html.parser")

    # detect table date
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", soup.get_text(" ", strip=True))
    table_date = (
        datetime.strptime(match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
        if match
        else fallback_date
    )

    # find the main table
    target_table = None
    for tbl in soup.find_all("table"):
        text = tbl.get_text(" ", strip=True).lower()
        if all(k in text for k in ["ngân", "tháng", "lãi suất"]):
            target_table = tbl
            break
    if not target_table:
        print("⚠️ No valid table found.")
        return None

    rows = target_table.find_all("tr")
    if len(rows) < 3:
        return None

    header_cells = rows[1].find_all("td")
    headers = ["bank"] + [
        re.sub(r"\s+", "_", c.get_text(strip=True).lower()) for c in header_cells[1:]
    ]

    data = []
    for row in rows[2:]:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        bank = cells[0].get_text(strip=True).upper()

        rates = []
        for c in cells[1:]:
            txt = c.get_text(strip=True).replace(",", ".")
            try:
                val = float(txt)
                if val > 100:
                    val = None
            except ValueError:
                val = None
            rates.append(val)

        record = {"date": table_date, "bank": bank}
        for i, h in enumerate(headers[1:]):
            record[h] = rates[i] if i < len(rates) else None
        data.append(record)

    df = pd.DataFrame(data)

    # normalise column names
    rename_map = {}
    for col in df.columns:
        col_clean = col.lower().strip()
        if "tháng" in col_clean:
            m = re.search(r"(\d+)", col_clean)
            if m:
                rename_map[col] = f"{m.group(1)}m"
        elif "không" in col_clean or "kỳ_hạn" in col_clean:
            rename_map[col] = "no_term"
    df = df.rename(columns=rename_map)

    for c in [x for x in df.columns if re.match(r"^\d+m$", x) or x == "no_term"]:
        df[c] = (
            df[c]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.strip()
        )
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df.loc[df[c] > 100, c] = None

    df = df.where(pd.notnull(df), None)
    return df


# ============================================================
# SCRAPER — PARQUET ONLY
# ============================================================
async def scrape_vietnamnet_interest_to_parquet(headless=True):
    """Scrape latest Vietnamnet deposit rate data and save only parquet."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        page = await browser.new_page()

        base = "https://vietnamnet.vn"
        page_num = 1
        today = datetime.now().strftime("%d%m%y")
        parquet_path = SAVE_DIR / f"deposit_rate_individual_{today}.parquet"
        all_data = []

        while True:
            url = f"https://vietnamnet.vn/tim-kiem-p{page_num}?q=Lãi suất ngân hàng hôm nay&od=2&bydaterang=all&newstype=all"
            print(f"🔍 Page {page_num}")
            if not await safe_goto(page, url):
                break
            try:
                await page.wait_for_selector("h3.vnn-title a", timeout=15000)
            except TimeoutError:
                print("⚠️ No search results found.")
                break

            soup = BeautifulSoup(await page.content(), "html.parser")
            links = []
            for a in soup.select("h3.vnn-title a[href]"):
                title = a.get_text(strip=True)
                href = a["href"]
                link = href if href.startswith("http") else base + href
                if "Lãi suất ngân hàng hôm nay" in title:
                    links.append(link)

            if not links:
                break

            for link in links[:1]:  # only need the latest article
                print(f"📄 Fetching {link}")
                if await safe_goto(page, link):
                    await page.wait_for_selector("table", timeout=20000)
                    html = await page.content()
                    df = parse_interest_table(html, datetime.now().strftime("%Y-%m-%d"))
                    if df is not None and not df.empty:
                        all_data.append(df)
                        break  # only latest day needed
            break  # exit after first valid page

        await browser.close()

        if not all_data:
            print("⚠️ No valid data found.")
            return None

        df_final = pd.concat(all_data, ignore_index=True)
        df_final.to_parquet(parquet_path, index=False, compression="gzip")
        print(f"💾 Saved Parquet → {parquet_path} ({len(df_final)} rows)")
        return parquet_path


# ============================================================
# UPLOAD TO R2
# ============================================================
def upload_to_r2_deposit_rate():
    bucket = os.getenv("R2_BUCKET")
    prefix_main = "cafef_data/"
    prefix_backup_master = "cafef_data/cafef_data_backup/"
    prefix_backup_individual = "cafef_data/cafef_data_backup/deposit_rate/"

    ensure_folder_exists(bucket, prefix_main)
    ensure_folder_exists(bucket, prefix_backup_master)
    ensure_folder_exists(bucket, prefix_backup_individual)

    today = datetime.now().strftime("%d%m%y")
    parquet_individual = SAVE_DIR / f"deposit_rate_individual_{today}.parquet"
    parquet_master = SAVE_DIR / f"deposit_rate_{today}.parquet"

    if not parquet_individual.exists():
        print("⚠️ No parquet file found to upload.")
        return

    # For this setup, master = today's data (you can later merge history externally)
    os.rename(parquet_individual, parquet_master)

    # upload main file
    upload_to_r2(parquet_master, bucket, f"{prefix_main}{parquet_master.name}")
    print(f"☁️ Uploaded master → {prefix_main}{parquet_master.name}")

    # upload backups
    upload_to_r2(parquet_master, bucket, f"{prefix_backup_master}{parquet_master.name}")
    clean_old_backups_r2(bucket, prefix_backup_master, keep=2)

    # upload individual parquet (same file, renamed for backup structure)
    upload_to_r2(parquet_master, bucket, f"{prefix_backup_individual}deposit_rate_individual_{today}.parquet")
    print(f"✅ Uploaded backups for {today}")


# ============================================================
# MAIN ENTRY POINT
# ============================================================
if __name__ == "__main__":
    parquet_path = asyncio.run(scrape_vietnamnet_interest_to_parquet(headless=True))
    if parquet_path:
        upload_to_r2_deposit_rate()