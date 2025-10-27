import os
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
)

# ============================================================
# CONFIG
# ============================================================
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path(os.getcwd()).resolve()

SAVE_DIR = BASE_DIR / "deposit_rate"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

BUCKET = os.getenv("R2_BUCKET")
PREFIX_MAIN = "cafef_data/"  # master lives here
PREFIX_BACKUP_MASTER = "cafef_data/cafef_data_backup/"  # keep last 2 master files
PREFIX_BACKUP_INDIV = "cafef_data/cafef_data_backup/deposit_rate/"  # all daily individual files

SEARCH_URL_TEMPLATE = (
    "https://vietnamnet.vn/tim-kiem-p{page}?q=Lãi suất ngân hàng hôm nay&od=2&bydaterang=all&newstype=all"
)

# ============================================================
# SMALL HELPERS
# ============================================================

def yyyymmdd_from_ddmmyyyy(s):
    # "25/10/2025" -> "2025-10-25"
    return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")

def ddmmyy_from_yyyy_mm_dd(s):
    # "2025-10-25" -> "251025"
    return datetime.strptime(s, "%Y-%m-%d").strftime("%d%m%y")

def extract_article_date_from_title(title_text):
    """
    Tries to find a dd/mm/yyyy in the article title and return 'YYYY-MM-DD'
    If not found: return None
    """
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", title_text)
    if not m:
        return None
    return yyyymmdd_from_ddmmyyyy(m.group(1))

def safe_float(cell_text):
    """
    Turn cell text like '4,65' or '4.7%' into float 4.65
    Return None if cannot parse or >100 (nonsense)
    """
    txt = cell_text.strip().replace(",", ".").replace("%", "")
    try:
        val = float(txt)
        if val > 100:
            return None
        return val
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

def normalize_rate_table(html, fallback_date):
    """
    Parse a vietnamnet interest-rate table HTML → pandas.DataFrame
    Columns:
      date, bank, no_term, 1m, 3m, 6m, 12m, ...
    """
    soup = BeautifulSoup(html, "html.parser")

    # detect date in page body; fallback to article date string if not found
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", soup.get_text(" ", strip=True))
    table_date = (
        yyyymmdd_from_ddmmyyyy(match.group(1))
        if match
        else fallback_date
    )

    # find "main" table
    target_table = None
    for tbl in soup.find_all("table"):
        text = tbl.get_text(" ", strip=True).lower()
        # crude heuristic: banking rates table tends to include these keywords
        if all(k in text for k in ["ngân", "tháng", "lãi suất"]):
            target_table = tbl
            break

    if target_table is None:
        print("⚠️ No valid table found in this article.")
        return None

    rows = target_table.find_all("tr")
    if len(rows) < 3:
        print("⚠️ Table is too short (<3 rows).")
        return None

    # header row typically at rows[1]
    header_cells = rows[1].find_all(["td", "th"])
    headers = ["bank"] + [
        re.sub(r"\s+", "_", c.get_text(strip=True).lower()) for c in header_cells[1:]
    ]

    data_rows = []
    for row in rows[2:]:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        bank_name = cells[0].get_text(strip=True).upper()
        rate_values = []
        for c in cells[1:]:
            rate_values.append(safe_float(c.get_text(strip=True)))

        rec = {"date": table_date, "bank": bank_name}
        for i, h in enumerate(headers[1:]):
            rec[h] = rate_values[i] if i < len(rate_values) else None
        data_rows.append(rec)

    df = pd.DataFrame(data_rows)

    # rename columns like "3_tháng" → "3m", "không_kỳ_hạn" → "no_term"
    rename_map = {}
    for col in df.columns:
        cclean = col.lower().strip()
        # map tenor columns
        if "tháng" in cclean:
            m = re.search(r"(\d+)", cclean)
            if m:
                rename_map[col] = f"{m.group(1)}m"
        # map no-term column
        if "không" in cclean or "kỳ_hạn" in cclean:
            rename_map[col] = "no_term"

    df = df.rename(columns=rename_map)

    # final numeric cleanup: ensure all tenor cols are numeric and <=100
    tenor_cols = [c for c in df.columns if re.match(r"^\d+m$", c) or c == "no_term"]
    for c in tenor_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df.loc[df[c] > 100, c] = None

    # make sure we don't get NaNs to object dtype surprises later
    df = df.where(pd.notnull(df), None)

    return df


def get_existing_parquet_dates_local():
    """
    Look in SAVE_DIR for deposit_rate_individual_<DDMMYY>.parquet.
    Return set of "YYYY-MM-DD" we've already saved.
    """
    existing = set()
    for p in SAVE_DIR.glob("deposit_rate_individual_*.parquet"):
        m = re.search(r"deposit_rate_individual_(\d{6})\.parquet$", p.name)
        if not m:
            continue
        ddmmyy = m.group(1)  # e.g. "251025"
        # turn DDMMYY -> YYYY-MM-DD
        dt = datetime.strptime(ddmmyy, "%d%m%y").strftime("%Y-%m-%d")
        existing.add(dt)
    return existing


def get_existing_parquet_dates_r2():
    """
    Check R2 under PREFIX_BACKUP_INDIV for deposit_rate_individual_<DDMMYY>.parquet
    so we don't re-scrape days we already uploaded.
    """
    keys = list_r2_files(BUCKET, PREFIX_BACKUP_INDIV)
    existing = set()
    for key in keys:
        m = re.search(r"deposit_rate_individual_(\d{6})\.parquet$", key)
        if not m:
            continue
        ddmmyy = m.group(1)
        dt = datetime.strptime(ddmmyy, "%d%m%y").strftime("%Y-%m-%d")
        existing.add(dt)
    return existing


async def scrape_vietnamnet_interest_range(start_date=None, end_date=None, headless=True):
    """
    Scrape interest-rate articles in a date range.
    - If start_date/end_date not given: just scrape today's latest.
    - If given: walk pages, collect all dates within [start_date, end_date].
      Stop early if we go past start_date or reach last available page.
    For each day not already captured in parquet (local or R2), save
    deposit_rate_individual_<DDMMYY>.parquet.
    Returns list of all local parquet paths we produced/confirmed for that run.
    """

    # --- parse range boundaries ---
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    # --- collect existing files (local + R2) ---
    already_have = get_existing_parquet_dates_local() | get_existing_parquet_dates_r2()
    scraped_parquet_paths = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        page = await browser.new_page()

        page_num = 1
        keep_scraping = True

        while keep_scraping:
            search_url = SEARCH_URL_TEMPLATE.format(page=page_num)
            print(f"\n🔍 Scanning search page {page_num} → {search_url}")
            ok = await safe_goto(page, search_url)
            if not ok:
                print("⚠️ Could not load search page, stopping.")
                break

            # --- wait for articles to appear ---
            try:
                await page.wait_for_selector("h3.vnn-title a", timeout=15000)
            except PlaywrightTimeoutError:
                print("🧭 Reached last page — no search results found.")
                break

            soup = BeautifulSoup(await page.content(), "html.parser")

            # --- extract all article links on this page ---
            articles = []
            for a in soup.select("h3.vnn-title a[href]"):
                title = a.get_text(strip=True)
                href = a["href"]
                full_link = href if href.startswith("http") else f"https://vietnamnet.vn{href}"
                art_date_str = extract_article_date_from_title(title)
                if not art_date_str:
                    continue
                art_dt = datetime.strptime(art_date_str, "%Y-%m-%d").date()

                # --- range filtering ---
                if end_dt and art_dt > end_dt:
                    continue
                if start_dt and art_dt < start_dt:
                    print(f"⏹️ Reached older than {start_date} (found {art_date_str}) → stopping.")
                    keep_scraping = False
                    break

                articles.append((art_dt, art_date_str, full_link))

            # --- stop if no more results ---
            if not articles:
                print(f"🧭 Page {page_num} has no valid interest-rate articles → end of archive.")
                break

            # --- no-date mode: only scrape today's newest ---
            if start_dt is None and end_dt is None:
                articles.sort(reverse=True)
                art_dt, art_date_str, link = articles[0]
                if art_date_str in already_have:
                    print(f"✔️ Already have {art_date_str}, skipping scrape.")
                else:
                    path = await scrape_single_article_to_parquet(page, link, art_date_str)
                    if path:
                        scraped_parquet_paths.append(path)
                break

            # --- historical range mode ---
            for art_dt, art_date_str, link in articles:
                if art_date_str in already_have:
                    print(f"✔️ {art_date_str} already exists, skip.")
                    continue
                path = await scrape_single_article_to_parquet(page, link, art_date_str)
                if path:
                    scraped_parquet_paths.append(path)

            # --- move to next page ---
            page_num += 1
            await asyncio.sleep(1)

        await browser.close()

    print(f"\n✅ Finished scanning. Total new daily files: {len(scraped_parquet_paths)}")
    return scraped_parquet_paths


async def scrape_single_article_to_parquet(page, article_url, date_str):
    """
    Go to one vietnamnet article URL, parse table, save parquet for that date.
    date_str is 'YYYY-MM-DD'.
    Returns the parquet path if saved, else None.
    """
    print(f"📄 Fetching article {article_url} (date {date_str})")

    ok = await safe_goto(page, article_url)
    if not ok:
        print(f"⚠️ Failed nav to {article_url}")
        return None

    try:
        await page.wait_for_selector("table", timeout=20000)
    except PlaywrightTimeoutError:
        print("⚠️ No table found in article.")
        return None

    html = await page.content()
    df_day = normalize_rate_table(html, date_str)
    if df_day is None or df_day.empty:
        print("⚠️ Parsed table is empty.")
        return None

    # save parquet as deposit_rate_individual_<DDMMYY>.parquet
    ddmmyy = ddmmyy_from_yyyy_mm_dd(date_str)
    parquet_path = SAVE_DIR / f"deposit_rate_individual_{ddmmyy}.parquet"
    df_day.to_parquet(parquet_path, index=False, compression="gzip")
    print(f"💾 Saved {parquet_path} ({len(df_day)} rows)")

    return parquet_path


def build_master_parquet(master_path):
    """
    Read every deposit_rate_individual_*.parquet in SAVE_DIR,
    concat, drop duplicates (date+bank), sort by date+bank,
    and save as master parquet.
    """
    parts = []
    for p in SAVE_DIR.glob("deposit_rate_individual_*.parquet"):
        try:
            dfp = pd.read_parquet(p)
            parts.append(dfp)
        except Exception as e:
            print(f"⚠️ Could not read {p}: {e}")

    if not parts:
        print("⚠️ No daily parquet files found to build master.")
        return None

    big = pd.concat(parts, ignore_index=True)

    # drop duplicate rows by same (date, bank)
    if "date" in big.columns and "bank" in big.columns:
        big = big.drop_duplicates(subset=["date", "bank"])

    # sort by date then bank for consistency
    big = big.sort_values(["date", "bank"]).reset_index(drop=True)

    big.to_parquet(master_path, index=False, compression="gzip")
    print(f"🏦 Master parquet saved → {master_path} ({len(big)} rows)")
    return master_path


def sync_to_r2(today_ddmmyy, master_path):
    """
    Uploads:
    - master parquet -> cafef_data/deposit_rate_<DDMMYY>.parquet
    - master parquet -> cafef_data/cafef_data_backup/deposit_rate_<DDMMYY>.parquet (keep 2 latest)
    - individual day parquet(s) -> cafef_data/cafef_data_backup/deposit_rate/*.parquet (append-only)
    Also trims backup masters to keep=2.
    """

    # ensure folders exist in R2
    ensure_folder_exists(BUCKET, PREFIX_MAIN)
    ensure_folder_exists(BUCKET, PREFIX_BACKUP_MASTER)
    ensure_folder_exists(BUCKET, PREFIX_BACKUP_INDIV)

    # 1) upload master to main
    main_key = f"{PREFIX_MAIN}deposit_rate_{today_ddmmyy}.parquet"
    upload_to_r2(master_path, BUCKET, main_key)
    print(f"☁️ Uploaded master → {main_key}")

    # 2) upload master to backup_master, then prune
    backup_master_key = f"{PREFIX_BACKUP_MASTER}deposit_rate_{today_ddmmyy}.parquet"
    upload_to_r2(master_path, BUCKET, backup_master_key)
    clean_old_backups_r2(BUCKET, PREFIX_BACKUP_MASTER, keep=2)

    # 3) upload individual daily files to backup/deposit_rate
    for p in SAVE_DIR.glob("deposit_rate_individual_*.parquet"):
        upload_to_r2(
            p,
            BUCKET,
            f"{PREFIX_BACKUP_INDIV}{p.name}"
        )
    print("📤 Synced individual daily files as well.")


# ============================================================
# PUBLIC ENTRYPOINT
# ============================================================

def run_deposit_rate_scraper(start_date=None, end_date=None, headless=True):
    """
    High-level flow:
    1. scrape_vietnamnet_interest_range(...) to create missing daily parquet(s)
    2. build/update master parquet from all local dailies
    3. upload master + daily parquets to R2 (and rotate backups)
    """

    # step 1: scrape + save any missing daily parquet(s)
    scraped_paths = asyncio.run(
        scrape_vietnamnet_interest_range(
            start_date=start_date,
            end_date=end_date,
            headless=headless,
        )
    )

    if scraped_paths:
        print(f"🆕 Got {len(scraped_paths)} new daily parquet file(s).")
    else:
        print("ℹ️ No new parquet scraped (all dates already exist).")

    # define today's tag (DDMMYY) for naming master
    # if you passed a range, we'll use end_date as "today" tag so you know which run it came from
    tag_date = end_date if end_date else datetime.now().strftime("%Y-%m-%d")
    today_ddmmyy = datetime.strptime(tag_date, "%Y-%m-%d").strftime("%d%m%y")

    master_path = SAVE_DIR / f"deposit_rate_{today_ddmmyy}.parquet"

    # step 2: build master parquet from all daily files
    master_path = build_master_parquet(master_path)
    if master_path is None:
        print("❌ No master created, skipping R2 sync.")
        return

    #step 3: push to R2
    sync_to_r2(today_ddmmyy, master_path)
    print("✅ Deposit rate sync completed.")




# ============================================================
# OPTIONAL MANUAL RUN EXAMPLES
# ============================================================
# Example 1: only today (latest article only)
# run_deposit_rate_scraper()

# Example 2: historical range (scrape between 2025-09-01 and 2025-10-25)
run_deposit_rate_scraper(start_date="2023-01-01")