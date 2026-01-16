# ============================================================
# vietnamnet_interest_rate.py - JSON/CSV version
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
import json
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from utils_r2 import (
    upload_to_r2,
    download_from_r2,
    ensure_folder_exists,
    list_r2_files,
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

# Using zero-based index for pagination found by browser analysis
# Page 1 -> p0, Page 2 -> p1
# URL Encode: "Lãi suất ngân hàng hôm nay" -> "Lãi+suất+ngân+hàng+hôm+nay"
SEARCH_URL_TEMPLATE = (
    "https://vietnamnet.vn/tim-kiem-p{index}?q=Lãi+suất+ngân+hàng+hôm+nay&od=2&bydaterang=all&newstype=all"
)

# ============================================================
# 3️⃣ HELPER FUNCTIONS
# ============================================================
def yyyymmdd_from_ddmmyyyy(s):
    return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")

def extract_article_date_from_title(title_text):
    # Try full date first: dd/mm/yyyy
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", title_text)
    if m:
        return yyyymmdd_from_ddmmyyyy(m.group(1))
    
    # Try short date: dd/mm (e.g. "17/12")
    m_short = re.search(r"(\d{1,2}/\d{1,2})", title_text)
    if m_short:
        day_month = m_short.group(1)
        current_year = datetime.now().year
        # Construct date with current year
        try:
            full_date_str = f"{day_month}/{current_year}"
            dt = datetime.strptime(full_date_str, "%d/%m/%Y").date()
            
            # Smart Year Inference:
            # If the date is in the future (e.g. Scrape in Jan 2026, find "25/12"), 
            # it implies the previous year.
            if dt > datetime.now().date():
                full_date_str = f"{day_month}/{current_year - 1}"
                # Re-parse
                dt = datetime.strptime(full_date_str, "%d/%m/%Y").date()
            
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    return None

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

    # User Request: Use the date found in the name (title).
    # fallback_date IS the date from the title (passed from collect_links).
    # So we prefer fallback_date if it exists. Only search body if fallback_date is None.
    
    table_date = None
    if fallback_date:
        table_date = fallback_date
    else:
        match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", soup.get_text(" ", strip=True))
        if match:
            table_date = yyyymmdd_from_ddmmyyyy(match.group(1))
    
    # If still None (shouldn't happen if title had date), then maybe we skip or keep scanning?
    # Logic implies table_date must be set.


    target_table = None
    for tbl in soup.find_all("table"):
        text = tbl.get_text(" ", strip=True).lower()
        if all(k in text for k in ["ngân", "tháng", "lãi suất"]):
            # Check row count immediately to filter out summary tables
            if len(tbl.find_all("tr")) >= 3:
                target_table = tbl
                break
            else:
                # Found matching keywords but too few rows (likely summary)
                # Continue searching for better table
                pass

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
        # Normalize bank name: uppercase and strip whitespace
        bank_name = cells[0].get_text(strip=True).upper().strip()
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
# 5️⃣ LOAD EXISTING JSON DATA
# ============================================================
def load_existing_json_data(bucket, prefix):
    """Load existing deposit rate JSON from R2."""
    files = list_r2_files(bucket, prefix)
    # Filter for files matching pattern deposit_rate_YYMMDD.json
    json_files = [f for f in files if re.search(r"deposit_rate_\d{6}\.json$", f)]
    
    # Sort to get the latest one (YYMMDD format sorts correctly as string if year is consistent 2-digit, 
    # but safer to parse date if needed. Given format 251227, string sort works for same decade)
    json_files.sort(reverse=True)
    
    if not json_files:
        print("ℹ️ No existing deposit rate JSON found on R2.")
        return pd.DataFrame()
    
    json_key = json_files[0]
    local_json = SAVE_DIR / "temp_deposit_rate.json"
    
    if download_from_r2(bucket, json_key, str(local_json)):
        try:
            with open(local_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data.get("data", []))
            print(f"✅ Loaded {len(df)} existing deposit rate records from R2.")
            
            # Clean up
            os.remove(local_json)
            return df
        except Exception as e:
            print(f"⚠️ Error loading JSON: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()


def get_existing_json_dates(bucket, prefix):
    """Get set of dates already in JSON."""
    existing_df = load_existing_json_data(bucket, prefix)
    if existing_df.empty:
        return set()
    return set(existing_df["date"].unique())


# ============================================================
# 6️⃣ SCRAPING LOGIC
# ============================================================
async def scrape_single_article(page, url, date_str):
    """Scrape a single article and return DataFrame."""
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

    print(f"✅ Scraped {len(df_day)} records for {date_str}")
    return df_day


async def scrape_vietnamnet_interest_range(start_date=None, end_date=None, headless=True):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    already_have = get_existing_json_dates(BUCKET, PREFIX_MAIN)
    scraped_dfs = []

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
                    print(f"⏭️ Skipping {date} (already have)")
                    continue
                df = await scrape_single_article(page, link, date)
                if df is not None:
                    scraped_dfs.append(df)
            page_num += 1
            await asyncio.sleep(1)
        await browser.close()

    print(f"✅ Total new dates scraped: {len(scraped_dfs)}")
    return scraped_dfs


# ============================================================
# 7️⃣ GENERATE JSON AND CSV
# ============================================================
def generate_deposit_json(df, output_path):
    """Generate JSON file from deposit rate DataFrame."""
    # Convert DataFrame to list of dictionaries
    records = df.to_dict(orient="records")
    
    json_data = {
        "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "total_records": len(records),
        "data": records
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Saved JSON → {output_path} ({len(records)} records)")
    return output_path


def generate_deposit_csv(df, output_path):
    """Generate CSV with date and tenor as first columns, banks as remaining columns."""
    # Normalize bank names to uppercase to avoid duplicates (VPBank vs VPBANK)
    df = df.copy()
    df['bank'] = df['bank'].str.upper().str.strip()
    
    # Get all unique tenor columns (including no_term for "không kỳ hạn")
    # Filter out columns that are completely empty (all NaN/None)
    raw_tenor_cols = [c for c in df.columns if re.match(r"^\d+m$", c) or c == "no_term"]
    tenor_cols = sorted([c for c in raw_tenor_cols if df[c].notna().any()])
    
    if not tenor_cols:
        print("⚠️ No tenor columns found for CSV")
        return None
    
    # Create list to store rows
    all_rows = []
    
    # For each date (Sorted Descending: Newest to Oldest)
    # User requested: "earliest to oldest" (interpreted as Latest -> Oldest based on context implies change)
    for date in sorted(df['date'].unique(), reverse=True):
        date_df = df[df['date'] == date]
        
        # For each tenor
        for tenor in tenor_cols:
            if tenor not in date_df.columns:
                continue
            
            # Create row for this date and tenor
            row_data = {'date': date, 'tenor': tenor}
            
            # Add each bank's rate for this tenor
            for _, bank_row in date_df.iterrows():
                bank = bank_row['bank']
                rate = bank_row.get(tenor, None)
                # Keep rate if not None/NaN
                if pd.notna(rate) and rate != "":
                    row_data[bank] = rate
            
            # Filter Check: Only add row if AT LEAST ONE bank has data for this tenor
            # Keys other than 'date' and 'tenor' represent banks
            has_data = any(k not in ['date', 'tenor'] and row_data[k] is not None for k in row_data)
            
            if has_data:
                all_rows.append(row_data)
    
    # Convert to DataFrame
    result = pd.DataFrame(all_rows)
    
    # Ensure date and tenor are first columns
    # User Request: Move banks with less data to the far right.
    # We sort bank columns by "Count of Non-Null Values" (Descending), then Alphabetical.
    if not result.empty:
        non_bank_cols = ['date', 'tenor']
        bank_cols = [c for c in result.columns if c not in non_bank_cols]
        
        # Sort key: (-count, name) -> Most data first, then A-Z
        bank_cols_sorted = sorted(bank_cols, key=lambda c: (-result[c].count(), c))
        
        cols = non_bank_cols + bank_cols_sorted
        result = result[cols]
    
    # Save to CSV
    # Use 'utf-8-sig' for Excel to correctly read Vietnamese characters
    result.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    unique_dates = len(result['date'].unique()) if not result.empty else 0
    unique_tenors = len(result['tenor'].unique()) if not result.empty else 0
    unique_banks = len([c for c in result.columns if c not in ['date', 'tenor']]) if not result.empty else 0
    
    print(f"💾 Saved CSV → {output_path} ({unique_dates} dates × {unique_tenors} tenors × {unique_banks} banks)")
    return output_path


# ============================================================
# 8️⃣ SYNC TO R2
# ============================================================
def sync_to_r2(combined_df, local_only=False):
    """Generate and upload JSON and CSV files to R2."""
    
    # Get latest date from data for filename
    latest_date = pd.to_datetime(combined_df["date"]).max()
    date_suffix = latest_date.strftime("%y%m%d")
    
    # Generate JSON
    json_path = SAVE_DIR / f"deposit_rate_{date_suffix}.json"
    generate_deposit_json(combined_df, json_path)
    
    # Generate CSV
    csv_path = SAVE_DIR / f"deposit_rate_{date_suffix}.csv"
    generate_deposit_csv(combined_df, csv_path)

    # Upload to R2 (skip if local_only)
    # Upload to R2 (skip if local_only)
    try:
        if not local_only and BUCKET:
            # Upload new files
            ensure_folder_exists(BUCKET, PREFIX_MAIN)
            
            json_r2_key = f"{PREFIX_MAIN}deposit_rate/deposit_rate_{date_suffix}.json"
            csv_r2_key = f"{PREFIX_MAIN}deposit_rate/deposit_rate_{date_suffix}.csv"

            from utils_r2 import upload_to_r2, backup_existing_file
            
            # Backup existing files before upload (safety first!)
            backup_existing_file(BUCKET, json_r2_key)
            backup_existing_file(BUCKET, csv_r2_key)
            
            upload_to_r2(str(json_path), BUCKET, json_r2_key)
            upload_to_r2(str(csv_path), BUCKET, csv_r2_key)
            print(f"📤 All deposit rate files synced to R2 with date suffix: {date_suffix}")

            # Clean old backups (keep only 1)
            print("🧹 Cleaning old backups for deposit_rate in R2...")
            from utils_r2 import backup_and_cleanup_r2
            backup_and_cleanup_r2(BUCKET, f"{PREFIX_MAIN}deposit_rate/", keep=1)
        else:
            print(f"\n📁 Files saved locally:")
            print(f"   - {json_path}")
            print(f"   - {csv_path}")
            print(f"\n💡 Review the files, then run with R2 upload enabled.")

    finally:
        # Clean up local files
        try:
            for f in SAVE_DIR.glob("deposit_rate_*"):
                os.remove(f)
                print(f"🧹 Deleted local file: {f}")
            if SAVE_DIR.exists() and not any(SAVE_DIR.iterdir()):
                SAVE_DIR.rmdir()
                print(f"🗑️ Removed empty folder: {SAVE_DIR}")
        except Exception as e:
            print(f"⚠️ Cleanup error: {e}")


# ============================================================
# 9️⃣ ENTRYPOINT
# ============================================================
def run_deposit_rate_scraper(start_date=None, end_date=None, headless=True, local_only=False, parallel=False, workers=5):
    """
    Unified flow: scrape deposit rates and save incrementally.
    
    Args:
        parallel: If True, use multi-worker parallel scraping (faster).
                  If False, use sequential scraping (more stable).
        workers: Number of parallel workers (only used if parallel=True).
    """
    
    # --- Helper to clean up old local files ---
    def cleanup_old_local_files(keep_suffix):
        """Delete local JSON/CSV files that don't match the keep_suffix."""
        for f in SAVE_DIR.glob("deposit_rate_*.json"):
            if f.name != f"deposit_rate_{keep_suffix}.json":
                try:
                    f.unlink()
                except: pass
        for f in SAVE_DIR.glob("deposit_rate_*.csv"):
            if f.name != f"deposit_rate_{keep_suffix}.csv":
                try:
                    f.unlink()
                except: pass

    # --- Load existing data (from R2 or local files) ---
    if local_only or not BUCKET:
        print("📍 Running in LOCAL-ONLY mode (no R2 operations)")
        # Try to load from local JSON file if it exists
        local_json_files = list(SAVE_DIR.glob("deposit_rate_*.json"))
        if local_json_files:
            # Load the most recent one
            latest_file = max(local_json_files, key=lambda p: p.stat().st_mtime)
            print(f"📂 Loading existing data from {latest_file.name}")
            try:
                import json
                with open(latest_file, 'r') as f:
                    json_data = json.load(f)
                existing_df = pd.DataFrame(json_data['data'])
                already_have = set(existing_df['date'].unique())
                print(f"✅ Loaded {len(existing_df)} existing records, {len(already_have)} unique dates")
            except Exception as e:
                print(f"⚠️ Error loading local JSON: {e}")
                existing_df = pd.DataFrame()
                already_have = set()
        else:
            print("📂 No existing local files found, starting fresh")
            existing_df = pd.DataFrame()
            already_have = set()
    else:
        # Load existing data from R2
        print("☁️ Loading existing data from R2")
        existing_df = load_existing_json_data(BUCKET, PREFIX_MAIN)
        already_have = get_existing_json_dates(BUCKET, PREFIX_MAIN)

    # Initialize combined dataframe
    combined = existing_df.copy() if not existing_df.empty else pd.DataFrame()

    # --- Definition of Sequential Scraper ---
    async def scrape_and_save_incrementally():
        nonlocal combined, already_have
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            page = await browser.new_page()
            page_num = 1
            keep = True
            total_scraped = 0

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
                elements = soup.select("h3.vnn-title a")
                print(f"   Debug: Found {len(elements)} raw elements. First one: {elements[0].get_text(strip=True) if elements else 'None'}")
                
                links = []
                for a in elements:
                    title, href = a.get_text(strip=True), a["href"]
                    date = extract_article_date_from_title(title)
                    if not date:
                         print(f"   ⚠️ Date extract failed for: {title}")
                         continue
                    dt = datetime.strptime(date, "%Y-%m-%d").date()
                    if start_dt and dt < start_dt:
                        keep = False
                        break
                    if end_dt and dt > end_dt:
                        continue
                    
                    # Auto-stop if we hit a date we already have (and no specific start_date was forced)
                    # Assumes articles are ordered newer-to-older.
                    if not start_dt and already_have:
                         try:
                             # already_have stores strings "YYYY-MM-DD"
                             max_existing_str = max(already_have) 
                             stop_dt_implicit = datetime.strptime(max_existing_str, "%Y-%m-%d").date()
                             if dt <= stop_dt_implicit:
                                 keep = False
                                 break
                         except: pass

                    link = href if href.startswith("http") else f"https://vietnamnet.vn{href}"
                    links.append((dt, date, link))
                if not links:
                    break
                    
                for dt, date, link in links:
                    if date in already_have:
                        print(f"⏭️ Skipping {date} (already have)")
                        # Optimization: If we didn't enforce a start_date, and we hit known data,
                        # AND we assume time-ordered content, we can probably stop pagination here.
                        # But strict safety leans on the check above.
                        # Let's add a "consecutive skip" check or strict date comparison?
                        
                        # Better: If start_date was NONE, we effectively imply start_date = max(already_have).
                        # So if we see a date that is <= max(already_have), we stop.
                        pass
                        continue
                    
                    # Scrape single article
                    df = await scrape_single_article(page, link, date)
                    if df is not None and not df.empty:
                        # Append to combined
                        if combined.empty:
                            combined = df
                        else:
                            combined = pd.concat([combined, df], ignore_index=True)
                            combined = combined.drop_duplicates(subset=["date", "bank"]).sort_values(["date", "bank"])
                        
                        # Mark as scraped
                        already_have.add(date)
                        total_scraped += len(df)
                        
                        # Save incrementally
                        latest_date = pd.to_datetime(combined["date"]).max()
                        date_suffix = latest_date.strftime("%y%m%d")
                        
                        json_path = SAVE_DIR / f"deposit_rate_{date_suffix}.json"
                        csv_path = SAVE_DIR / f"deposit_rate_{date_suffix}.csv"
                        
                        generate_deposit_json(combined, json_path)
                        generate_deposit_csv(combined, csv_path)
                        print(f"💾 Saved incrementally: {len(combined)} total records")
                        
                        # Clean up old files
                        cleanup_old_local_files(date_suffix)
                
                page_num += 1
                await asyncio.sleep(1)
            await browser.close()
            
            print(f"✅ Scraping complete: {total_scraped} new records scraped")

    # --- Definition of Parallel Scraper ---
    async def collect_links():
        """Phase 1: Collect all article links in the date range."""
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None
        
        all_links = []
        
        # Debug: Show stop criteria
        print(f"ℹ️ [Debug] Start Date: {start_dt}")
        print(f"ℹ️ [Debug] End Date:   {end_dt}")
        print(f"ℹ️ [Debug] Existing Dates Found: {len(already_have)}")
        
        if start_dt and end_dt and start_dt > end_dt:
            print(f"⚠️ WARNING: Start Date ({start_dt}) is newer than End Date ({end_dt}). You might get no results.")

        stop_dt_implicit = None
        # User requested "no specification -> parse all data"
        # So we DISABLE implicit stop based on local files.
        # We will scan everything unless --start_date is explicitly set.
        if not start_dt:
             print("ℹ️ [Debug] No Start Date specified. Scanning FULL history (ignoring already_have limits).")
        else:
             if not start_dt:
                 print("ℹ️ [Debug] No existing data found (or empty), so scanning ALL pages (fresh run).")

        
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            page = await browser.new_page()
            page_num = 1
            keep = True
            empty_pages_consecutive = 0

            while keep:
                # Calculate 0-based index
                page_index = page_num - 1
                search_url = SEARCH_URL_TEMPLATE.format(index=page_index)
                
                print(f"🔍 Collecting links from page {page_num} (p{page_index})...")
                if not await safe_goto(page, search_url):
                    break
                try:
                    await page.wait_for_selector("h3.vnn-title a", timeout=15000)
                except PlaywrightTimeoutError:
                    break
                    
                soup = BeautifulSoup(await page.content(), "html.parser")
                page_links = [] # Temporary list for this page
                
                # Debug: Print first 3 titles of page 1 to verify scraping
                debug_count = 0
                
                elements = soup.select("h3.vnn-title a")
                print(f"   Debug: Found {len(elements)} raw elements. First one: {elements[0].get_text(strip=True) if elements else 'None'}")

                duplicates_count = 0
                min_page_dt = None
                
                for a in elements:
                    title, href = a.get_text(strip=True), a["href"]
                    date = extract_article_date_from_title(title)
                    
                    if not date:
                         if len(title) > 15:
                             print(f"   ⚠️ Date extract failed for: {title}")
                         continue
                    dt = datetime.strptime(date, "%Y-%m-%d").date()
                    
                    # Track oldest date on page (regardless of duplication)
                    if min_page_dt is None or dt < min_page_dt:
                        min_page_dt = dt
                    
                    # Individual Item Check
                    if start_dt and dt < start_dt:
                        print(f"🛑 Found older date {dt} < {start_dt}. Stopping.")
                        keep = False
                        break
                    


                    link = href if href.startswith("http") else f"https://vietnamnet.vn{href}"
                    
                    # Skip if already have (redundant but safe)
                    if date in already_have:
                        duplicates_count += 1
                        continue
                        
                    all_links.append((date, link))
                    page_links.append(dt)
                
                # End of page safety check (User Request)
                if not page_links:
                    if duplicates_count > 0:
                        # Full page of duplicates.
                        # Rule: If start_date is set, we ONLY stop if we PASSED it.
                        # If start_date is NOT set, we CONTINUE (Full Scan).
                        
                        should_continue = True 
                        
                        if start_dt:
                            if min_page_dt and min_page_dt >= start_dt:
                                should_continue = True
                            else:
                                should_continue = False
                        
                        if should_continue:
                            print(f"⏭️ Page {page_num} duplicate, but Full Scan / Start Date active. Continuing...")
                            # Reset empty page counter if we found legitimate duplicates (it's a valid page, just old)
                            empty_pages_consecutive = 0
                        else:
                            print(f"✅ Page {page_num} contains only existing data. Stopping pagination (Start Date reached).")
                            keep = False
                            break
                    else:
                        # No valid dates found (likely irrelevant search results)
                        empty_pages_consecutive += 1
                        print(f"⚠️ Page {page_num} yielded no valid dates/articles. (Consecutive empty: {empty_pages_consecutive}/3)")
                        
                        if empty_pages_consecutive >= 3:
                            print("🛑 Too many consecutive empty pages. Stopping pagination.")
                            keep = False
                            break
                        else:
                            print("⏭️ Skipping empty page...")

                if page_links:
                    last_dt = page_links[-1]
                    
                    # User Request: Show comparison
                    if start_dt:
                        print(f"   (Oldest date on page: {last_dt} vs Start Date Limit: {start_dt})")
                    else:
                         print(f"   (Oldest date on page: {last_dt})")

                    if start_dt and last_dt < start_dt:
                         print(f"🛑 Last item on page {last_dt} < {start_dt}. Stopping page loop.")
                         keep = False
                    if stop_dt_implicit and last_dt <= stop_dt_implicit:
                         print(f"🛑 Last item on page {last_dt} <= {stop_dt_implicit}. Stopping page loop.")
                         keep = False
                
                if not keep:
                    break
                
                # Check if we found ANY dates on this page? 
                # If page was valid HTML but we extracted NO dates, we might be on a weird old page or layout changed.
                # Assuming if we find 0 dates, we should probably stop or it's a fluke?
                # For now let's just proceed to next page if keep is True.
                
                page_num += 1
                await asyncio.sleep(0.5)
            
            await browser.close()
        
        print(f"✅ Collected {len(all_links)} articles to scrape")
        return all_links

    async def scrape_worker(worker_id, links_queue, results_lock):
        """Worker function to scrape articles from a queue."""
        nonlocal combined
        
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            page = await browser.new_page()
            scraped = 0
            
            while not links_queue.empty():
                try:
                    date, link = links_queue.get_nowait()
                except:
                    break
                
                print(f"[Worker {worker_id}] 📄 Scraping {date}...")
                df = await scrape_single_article(page, link, date)
                
                if df is not None and not df.empty:
                    # Thread-safe update
                    async with results_lock:
                        if combined.empty:
                            combined = df
                        else:
                            combined = pd.concat([combined, df], ignore_index=True)
                            combined = combined.drop_duplicates(subset=["date", "bank"]).sort_values(["date", "bank"])
                        
                        already_have.add(date)
                        scraped += 1
                        
                        # Save incrementally (every article)
                        latest_date = pd.to_datetime(combined["date"]).max()
                        date_suffix = latest_date.strftime("%y%m%d")
                        
                        json_path = SAVE_DIR / f"deposit_rate_{date_suffix}.json"
                        csv_path = SAVE_DIR / f"deposit_rate_{date_suffix}.csv"
                        
                        generate_deposit_json(combined, json_path)
                        generate_deposit_csv(combined, csv_path)
                        print(f"[Worker {worker_id}] 💾 Saved: {len(combined)} total records")
                        
                        # Clean up old files
                        cleanup_old_local_files(date_suffix)
            
            await browser.close()
            print(f"[Worker {worker_id}] ✅ Completed {scraped} articles")

    async def run_parallel_scrape():
        """Phase 2: Scrape articles in parallel with multiple workers."""
        # Collect all links first
        links = await collect_links()
        
        if not links:
            print("ℹ️ No new articles to scrape")
            return
        
        # Create queue and lock
        import asyncio
        from queue import Queue
        queue = asyncio.Queue()
        for link_pair in links:
            queue.put_nowait(link_pair)
        
        # Async lock for thread-safe updates
        lock = asyncio.Lock()
        
        # Start workers
        tasks = [
            scrape_worker(i+1, queue, lock) 
            for i in range(min(workers, len(links)))
        ]
        
        await asyncio.gather(*tasks)
        print(f"✅ All workers completed")


    # Choose scraping mode
    if parallel:
        print(f"🚀 Running in PARALLEL mode with {workers} workers")
        asyncio.run(run_parallel_scrape())
    else:
        print(f"📍 Running in SEQUENTIAL mode")
        asyncio.run(scrape_and_save_incrementally())
    if combined.empty or len(combined) == len(existing_df):
        print("ℹ️ No new data scraped.")
        return
    
    # Final save with backup
    sync_to_r2(combined_df=combined, local_only=local_only)
    print("✅ Deposit rate sync completed.")


# ============================================================
# MANUAL RUN
# ============================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="VietnamNet Deposit Rate Scraper")
    parser.add_argument("--start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--parallel", action="store_true", default=True, help="Run in parallel mode")
    parser.add_argument("--no-parallel", dest="parallel", action="store_false", help="Run in sequential mode")
    parser.add_argument("--workers", type=int, default=10, help="Number of workers for parallel mode")
    parser.add_argument("--headless", action="store_true", default=True, help="Run headless")
    parser.add_argument("--no-headless", dest="headless", action="store_false", help="Run with browser UI")
    
    args = parser.parse_args()

    # Run in local-only mode if BUCKET is not set
    local_mode = not bool(BUCKET)
    
    run_deposit_rate_scraper(
        start_date=args.start_date,
        end_date=args.end_date,
        parallel=args.parallel,
        headless=args.headless,
        workers=args.workers,
        local_only=local_mode
    )