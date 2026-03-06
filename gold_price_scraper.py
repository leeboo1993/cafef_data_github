import os
import json
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from utils_r2 import upload_to_r2, download_from_r2, list_r2_files

# Load environment variables
load_dotenv()

BUCKET = os.getenv("R2_BUCKET", "broker-data")
SAVE_DIR = Path.cwd() / "precious_metals"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

URL_GOLD_HIST = "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=all"
URL_GOLD_RING_DOJI = "https://giavang.doji.vn/api/giavang/?api_key=258fbd2a72ce8481089d88c678e9fe4f"
URL_SILVER_PAGE = "https://cafef.vn/du-lieu/gia-bac-hom-nay/trong-nuoc.chn"

def fetch_gold_data():
    """Fetch gold data using stable AJAX endpoints."""
    print("💹 Fetching Gold data from AJAX...")
    results = {"date": datetime.now().strftime("%Y-%m-%d"), "bar_buy": None, "bar_sell": None, "ring_buy": None, "ring_sell": None}
    
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    
    # 1. SJC Bar Price (from history API)
    try:
        resp = requests.get(URL_GOLD_HIST, headers=headers, timeout=10)
        if resp.status_code == 200:
            content = resp.json()
            hist = content.get("Data", {}).get("goldPriceWorldHistories", [])
            if hist:
                latest = hist[0]
                # Price is in million VND per tael (e.g., 180.8)
                results["bar_buy"] = latest.get("buyPrice")
                results["bar_sell"] = latest.get("sellPrice")
    except Exception as e:
        print(f"  ⚠️ Error fetching gold history: {e}")

    # 2. Ring Price (from DOJI API)
    try:
        # Disable cert validation for DOJI API due to self-signed/expired certs on their internal endpoint
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        resp = requests.get(URL_GOLD_RING_DOJI, headers=headers, timeout=10, verify=False)
        if resp.status_code == 200:
            xml_text = resp.content.decode('utf-8-sig')
            root = ET.fromstring(xml_text)
            for row in root.findall(".//Row"):
                if row.attrib.get('Key') == 'nhanhung1chi' or "Nh" in row.attrib.get('Name', '') and "ng" in row.attrib.get('Name', ''):
                    buy_str = row.attrib.get('Buy', '').replace(',', '')
                    sell_str = row.attrib.get('Sell', '').replace(',', '')
                    if buy_str and sell_str:
                        buy = float(buy_str)
                        sell = float(sell_str)
                        # Value from DOJI is in 1000 VND / chi (e.g. 18380). Divide by 1000 to get Millions per Chi.
                        results["ring_buy"] = buy / 1000
                        results["ring_sell"] = sell / 1000
                    break
    except Exception as e:
        print(f"  ⚠️ Error fetching gold ring from DOJI: {e}")
        
    return results

def fetch_silver_data():
    """Fetch silver data by parsing the stable HTML structure."""
    print(f"🥈 Fetching Silver data from HTML: {URL_SILVER_PAGE}")
    results = {"date": datetime.now().strftime("%Y-%m-%d"), "silver_buy": None, "silver_sell": None}
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        resp = requests.get(URL_SILVER_PAGE, headers=headers, timeout=10)
        if resp.status_code == 200:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, 'lxml')
            
            items = soup.select(".list_silver_price_item")
            if not items:
                print("  ⚠️ Could not find silver price items on the page.")
                return results

            # Find the row corresponding to 1Kilo, or fallback to the first row (1 luong)
            kilo_found = False
            for div in items:
                cols = div.text.strip().split('\n')
                if len(cols) >= 4 and "kilo" in cols[0].lower():
                    buy = cols[1].strip()
                    sell = cols[3].strip()
                    if buy and sell:
                        # Kilo string prices are already in million VND / Kilogram
                        results["silver_buy"] = float(buy.replace(",", ""))
                        results["silver_sell"] = float(sell.replace(",", ""))
                        kilo_found = True
                        break

            if not kilo_found:
                # Fallback to the first row
                cols = items[0].text.strip().split('\n')
                if len(cols) >= 4:
                    buy = cols[1].strip()
                    sell = cols[3].strip()
                    if buy and sell:
                        # 1 luong price -> convert to million/kg
                        # Price per kg = (Price per tael) * 26.6667
                        results["silver_buy"] = float(buy.replace(",", "")) * 26.6667
                        results["silver_sell"] = float(sell.replace(",", "")) * 26.6667
                        
    except Exception as e:
        print(f"  ⚠️ Error fetching silver: {e}")
        
    return results

def load_existing_r2_data(prefix):
    files = list_r2_files(BUCKET, prefix)
    json_files = sorted([f for f in files if f.endswith(".json")], reverse=True)
    
    if not json_files:
        return pd.DataFrame()
    
    # Try the latest few to find valid data
    for latest_file in json_files[:5]:
        local_temp = SAVE_DIR / "temp_existing.json"
        if download_from_r2(BUCKET, latest_file, str(local_temp)):
            try:
                with open(local_temp, "r") as f:
                    content = json.load(f)
                    data = content.get("data", {})
                records = []
                for d_str, vals in data.items():
                    rec = {"date": pd.to_datetime(d_str)}
                    rec.update(vals)
                    records.append(rec)
                os.remove(local_temp)
                if records:
                    return pd.DataFrame(records)
            except:
                if local_temp.exists(): os.remove(local_temp)
                continue
    return pd.DataFrame()

def process_and_upload(category, new_record, prefix, filename_prefix, unit_desc):
    print(f"📈 Processing {category} data...")
    existing_df = load_existing_r2_data(prefix)
    
    new_df = pd.DataFrame([new_record])
    new_df["date"] = pd.to_datetime(new_df["date"])
    
    if not existing_df.empty:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        # Drop problematic columns if any, only keep relevant ones
        cols = ["date"] + [k for k in new_record.keys() if k != "date" and k != "all_sources"]
        combined = combined[cols]
        combined = combined.drop_duplicates(subset=["date"], keep="last").sort_values("date")
    else:
        cols = ["date"] + [k for k in new_record.keys() if k != "date" and k != "all_sources"]
        combined = new_df[cols]
        
    date_suffix = datetime.now().strftime("%y%m%d")
    json_filename = f"{filename_prefix}_{date_suffix}.json"
    json_path = SAVE_DIR / json_filename
    
    data_dict = {}
    for _, row in combined.iterrows():
        d_str = row["date"].strftime("%Y-%m-%d")
        vals = {k: v for k, v in row.to_dict().items() if k != "date" and pd.notnull(v)}
        data_dict[d_str] = vals
    
    json_out = {
        "last_updated": datetime.now().isoformat(),
        "total_records": len(data_dict),
        "units": unit_desc,
        "data": data_dict
    }
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_out, f, ensure_ascii=False, indent=2)
        
    upload_to_r2(str(json_path), BUCKET, f"{prefix}{json_filename}")
    print(f"✅ {category} uploaded: {json_filename}")

def update_precious_metals():
    gold_data = fetch_gold_data()
    silver_data = fetch_silver_data()
    
    if gold_data["bar_buy"] or gold_data["ring_buy"]:
        process_and_upload("Gold", gold_data, "cafef_data/gold_price/", "gold_price", "million_vnd")
        
    if silver_data["silver_buy"]:
        process_and_upload("Silver", silver_data, "cafef_data/silver_price/", "silver_price", "million_vnd_per_kg")

if __name__ == "__main__":
    update_precious_metals()
    # Cleanup
    for f in SAVE_DIR.glob("*.json"):
        os.remove(f)
    if SAVE_DIR.exists() and not any(SAVE_DIR.iterdir()):
        SAVE_DIR.rmdir()
    print("✅ Precious metals update completed.")