# ============================================================
# 0️⃣ UNIVERSAL ENV LOADER (works for both local & CI/CD)
# ============================================================
from pathlib import Path
import os

def load_env_safely():
    """Load .env if running locally (skip in CI/CD where env vars exist)."""
    if not os.getenv("R2_BUCKET"):
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            print("🔧 Loaded .env file (local environment).")
        else:
            print("⚠️ No .env found — expecting credentials in environment.")
    else:
        print("✅ Environment variables already loaded (CI/CD or Render).")

load_env_safely()

# ============================================================
# 1️⃣ IMPORTS
# ============================================================
import re
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from utils_r2 import (
    upload_to_r2,
    ensure_folder_exists,
    clean_old_backups_r2,
    list_r2_files,
    r2_client,
)

# ============================================================
# 2️⃣ CONFIG
# ============================================================
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path(os.getcwd()).resolve()

SAVE_DIR = BASE_DIR / "deposit_rate"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

BUCKET = os.getenv("R2_BUCKET")

PREFIX_MAIN = "cafef_data/"
PREFIX_BACKUP_MASTER = "cafef_data/cafef_data_backup/deposit_rate/master_file_backup/"
PREFIX_BACKUP_INDIV = "cafef_data/cafef_data_backup/deposit_rate/individual/"

SEARCH_URL_TEMPLATE = (
    "https://vietnamnet.vn/tim-kiem-p{page}?q=Lãi suất ngân hàng hôm nay&od=2&bydaterang=all&newstype=all"
)

# ============================================================
# 3️⃣ HELPER FUNCTIONS
# ============================================================
def extract_date_from_name(name):
    m = re.search(r"(\d{6})\.parquet$", name)
    return datetime.strptime(m.group(1), "%d%m%y") if m else None

def is_file_valid(bucket, key):
    s3 = r2_client()
    try:
        meta = s3.head_object(Bucket=bucket, Key=key)
        return meta["ContentLength"] > 0
    except Exception:
        return False

def get_latest_valid_file(bucket, prefix, pattern):
    """Return latest valid (key, date) matching pattern in prefix."""
    files = list_r2_files(bucket, prefix)
    pairs = [(f, extract_date_from_name(f)) for f in files if pattern in f]
    pairs = [(f, d) for f, d in pairs if d]
    if not pairs:
        return None, None
    latest, dt = max(pairs, key=lambda x: x[1])
    return (latest, dt) if is_file_valid(bucket, latest) else (None, None)

def yyyymmdd_from_ddmmyyyy(s):
    return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")

def ddmmyy_from_yyyy_mm_dd(s):
    return datetime.strptime(s, "%Y-%m-%d").strftime("%d%m%y")

def extract_article_date_from_title(title_text):
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", title_text)
    if not m:
        return None
    return yyyymmdd_from_ddmmyyyy(m.group(1))

def safe_float(cell_text):
    txt = cell_text.strip().replace(",", ".").replace("%", "")
    try:
        val = float(txt)
        return val if val <= 100 else None
    except:
        return None

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


# ============================================================
# 4️⃣ PARSING HTML TO DATAFRAME
# ============================================================
def normalize_rate_table(html, fallback_date):
    soup = BeautifulSoup(html, "html.parser")

    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", soup.get_text(" ", strip=True))
    table_date = yyyymmdd_from_ddmmyyyy(match.group(1)) if match else fallback_date

    target_table = None
    for tbl in soup.find_all("table"):
        text = tbl.get_text(" ", strip=True).lower()
        if all(k in text for k in ["ngân", "tháng", "lãi suất"]):
            target_table = tbl
            break

    if target_table is None:
        print("⚠️ No valid table found.")
        return None

    rows = target_table.find_all("tr")
    if len(rows) < 3:
        print("⚠️ Too few rows.")
        return None

    header_cells = rows[1].find_all(["td", "th"])
    headers = ["bank"] + [re.sub(r"\s+", "_", c.get_text(strip=True).lower()) for c in header_cells[1:]]

    data_rows = []
    for row in rows[2:]:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        bank_name = cells[0].get_text(strip=True).upper()
        rates = [safe_float(c.get_text(strip=True)) for c in cells[1:]]
        rec = {"date": table_date, "bank": bank_name}
        for i, h in enumerate(headers[1:]):
            rec[h] = rates[i] if i < len(rates) else None
        data_rows.append(rec)

    df = pd.DataFrame(data_rows)
    rename_map = {}
    for col in df.columns:
        c = col.lower()
        if "tháng" in c:
            m = re.search(r"(\d+)", c)
            if m:
                rename_map[col] = f"{m.group(1)}m"
        if "không" in c or "kỳ_hạn" in c:
            rename_map[col] = "no_term"
    df = df.rename(columns=rename_map)

    tenor_cols = [c for c in df.columns if re.match(r"^\d+m$", c) or c == "no_term"]
    for c in tenor_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df.loc[df[c] > 100, c] = None
    return df.where(pd.notnull(df), None)


# ============================================================
# 5️⃣ SCRAPING LOGIC
# ============================================================
def get_existing_parquet_dates_local():
    existing = set()
    for p in SAVE_DIR.glob("deposit_rate_individual_*.parquet"):
        m = re.search(r"deposit_rate_individual_(\d{6})\.parquet$", p.name)
        if not m:
            continue
        dt = datetime.strptime(m.group(1), "%d%m%y").strftime("%Y-%m-%d")
        existing.add(dt)
    return existing

def get_existing_parquet_dates_r2():
    keys = list_r2_files(BUCKET, PREFIX_BACKUP_INDIV)
    existing = set()
    for key in keys:
        m = re.search(r"deposit_rate_individual_(\d{6})\.parquet$", key)
        if not m:
            continue
        dt = datetime.strptime(m.group(1), "%d%m%y").strftime("%Y-%m-%d")
        existing.add(dt)
    return existing


async def scrape_single_article_to_parquet(page, url, date_str):
    print(f"📄 Fetching article {url} (date {date_str})")
    if not await safe_goto(page, url):
        return None
    try:
        await page.wait_for_selector("table", timeout=20000)
    except PlaywrightTimeoutError:
        return None

    html = await page.content()
    df_day = normalize_rate_table(html, date_str)
    if df_day is None or df_day.empty:
        return None

    ddmmyy = ddmmyy_from_yyyy_mm_dd(date_str)
    path = SAVE_DIR / f"deposit_rate_individual_{ddmmyy}.parquet"
    df_day.to_parquet(path, index=False, compression="gzip")
    print(f"💾 Saved {path}")
    return path


async def scrape_vietnamnet_interest_range(start_date=None, end_date=None, headless=True):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    already_have = get_existing_parquet_dates_local() | get_existing_parquet_dates_r2()
    scraped = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        page = await browser.new_page()
        page_num = 1
        keep = True

        while keep:
            search_url = SEARCH_URL_TEMPLATE.format(page=page_num)
            print(f"🔍 Scanning page {page_num}: {search_url}")
            if not await safe_goto(page, search_url):
                break
            try:
                await page.wait_for_selector("h3.vnn-title a", timeout=15000)
            except PlaywrightTimeoutError:
                break
            soup = BeautifulSoup(await page.content(), "html.parser")
            links = []
            for a in soup.select("h3.vnn-title a[href]"):
                title, href = a.get_text(strip=True), a["href"]
                date = extract_article_date_from_title(title)
                if not date:
                    continue
                dt = datetime.strptime(date, "%Y-%m-%d").date()
                if start_dt and dt < start_dt:
                    keep = False
                    break
                if end_dt and dt > end_dt:
                    continue
                link = href if href.startswith("http") else f"https://vietnamnet.vn{href}"
                links.append((dt, date, link))
            if not links:
                break
            for dt, date, link in links:
                if date in already_have:
                    continue
                p = await scrape_single_article_to_parquet(page, link, date)
                if p:
                    scraped.append(p)
            page_num += 1
            await asyncio.sleep(1)
        await browser.close()

    print(f"✅ Total new daily files: {len(scraped)}")
    return scraped


# ============================================================
# 6️⃣ BUILD MASTER + SYNC
# ============================================================
def build_master_parquet(master_path):
    parts = []
    for p in SAVE_DIR.glob("deposit_rate_individual_*.parquet"):
        try:
            parts.append(pd.read_parquet(p))
        except Exception as e:
            print(f"⚠️ Error reading {p}: {e}")
    if not parts:
        return None
    big = pd.concat(parts, ignore_index=True)
    big = big.drop_duplicates(subset=["date", "bank"]).sort_values(["date", "bank"])
    big.to_parquet(master_path, index=False, compression="gzip")
    print(f"🏦 Master parquet saved → {master_path}")
    return master_path


def sync_to_r2(today_ddmmyy, master_path):
    ensure_folder_exists(BUCKET, PREFIX_MAIN)
    ensure_folder_exists(BUCKET, PREFIX_BACKUP_MASTER)
    ensure_folder_exists(BUCKET, PREFIX_BACKUP_INDIV)

    main_key = f"{PREFIX_MAIN}deposit_rate_{today_ddmmyy}.parquet"
    upload_to_r2(master_path, BUCKET, main_key)
    upload_to_r2(master_path, BUCKET, f"{PREFIX_BACKUP_MASTER}deposit_rate_{today_ddmmyy}.parquet")
    clean_old_backups_r2(BUCKET, PREFIX_BACKUP_MASTER, keep=2)
    for p in SAVE_DIR.glob("deposit_rate_individual_*.parquet"):
        upload_to_r2(p, BUCKET, f"{PREFIX_BACKUP_INDIV}{p.name}")
    print("📤 All deposit rate files synced.")

    # 🧹 Local cleanup after upload (safe)
    try:
        for f in SAVE_DIR.glob("deposit_rate_*.parquet"):
            os.remove(f)
            print(f"🧹 Deleted local file: {f}")
        if not any(SAVE_DIR.iterdir()):
            SAVE_DIR.rmdir()
            print(f"🗑️ Removed empty folder: {SAVE_DIR}")
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")


# ============================================================
# 7️⃣ ENTRYPOINT
# ============================================================
def run_deposit_rate_scraper(start_date=None, end_date=None, headless=True):
    """Unified flow: skip if up-to-date, scrape new data, build + upload master."""
    latest_key, latest_dt = get_latest_valid_file(BUCKET, PREFIX_MAIN, "deposit_rate_")
    if latest_dt and latest_dt.date() >= datetime.now().date():
        print(f"✅ Already up-to-date ({latest_dt.strftime('%d/%m/%Y')}) — skip.")
        return None

    scraped = asyncio.run(scrape_vietnamnet_interest_range(start_date, end_date, headless))
    if not scraped:
        print("ℹ️ No new files scraped.")
    tag_date = end_date or datetime.now().strftime("%Y-%m-%d")
    today_ddmmyy = datetime.strptime(tag_date, "%Y-%m-%d").strftime("%d%m%y")
    master_path = SAVE_DIR / f"deposit_rate_{today_ddmmyy}.parquet"
    master = build_master_parquet(master_path)
    if master:
        sync_to_r2(today_ddmmyy, master)
        print("✅ Deposit rate sync completed.")


# ============================================================
# MANUAL RUN
# ============================================================
if __name__ == "__main__":
    run_deposit_rate_scraper()