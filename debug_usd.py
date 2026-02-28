
import requests
from bs4 import BeautifulSoup
import datetime

# Try fetching for a recent date (e.g. 15-01-2026 or today)
# Note: User complained data stopped at 26/12/2025.
# Let's try 15-01-2026.
date_str = "15-01-2026"

url = f"https://tygiausd.org/TyGia?date={date_str}"
print(f"Fetching {url}...")

try:
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    print(f"Status Code: {r.status_code}")
    
    if r.status_code != 200:
        print("Failed to fetch page")
    else:
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Check for specific elements logic uses
        date_input = soup.find("input", {"id": "date"})
        if date_input:
            print(f"Found input#date value: {date_input.get('value')}")
        else:
            print("❌ input#date NOT FOUND")
            
        strong_tag = soup.find("strong", string="USD tự do")
        if strong_tag:
            print("Found 'USD tự do' tag")
            row = strong_tag.find_parent("tr")
            if row:
                tds = row.find_all("td")
                print(f"Row has {len(tds)} tds")
                for i, td in enumerate(tds):
                    print(f"  TD {i}: {td.get_text(strip=True)}")
        else:
            print("❌ 'USD tự do' tag NOT FOUND")
            
            # Print generic table to see what is there
            print("\n--- ALL TABLES ---")
            for t in soup.find_all("table"):
                print(t.get_text(strip=True)[:200] + "...")

except Exception as e:
    print(f"Error: {e}")
