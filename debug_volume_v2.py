
import httpx
from bs4 import BeautifulSoup

url = "https://www.sbv.gov.vn/lãi-suất1"
print(f"Fetching {url}...")

try:
    client = httpx.Client(verify=False, follow_redirects=True, timeout=30)
    resp = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
    
    soup = BeautifulSoup(resp.content, "html.parser")
    
    print("\n--- All Headings (h1, h2, h3) ---")
    for h in soup.find_all(['h1', 'h2', 'h3']):
        print(f"[{h.name}] {h.get_text(strip=True)}")
    
    # Try more generic table search
    print("\n--- Tables ---")
    tables = soup.find_all("table")
    print(f"Found {len(tables)} tables")
    
    for i, table in enumerate(tables):
        print(f"\nTable {i+1}:")
        rows = table.find_all("tr")
        if not rows: continue
        
        # Print first few rows
        for j, row in enumerate(rows[:5]):
            cols = [td.get_text(" ", strip=True) for td in row.find_all(['td', 'th'])]
            print(f"   R{j}: {cols}")

except Exception as e:
    print(f"Error: {e}")
