
import httpx
from bs4 import BeautifulSoup

url = "https://www.sbv.gov.vn/lãi-suất1"
print(f"Fetching {url}...")

try:
    client = httpx.Client(verify=False, follow_redirects=True, timeout=30)
    resp = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
    
    soup = BeautifulSoup(resp.content, "html.parser")
    heading = soup.find(lambda tag: 
        tag.name in ("h2", "h3") and 
        "liên ngân hàng" in tag.get_text(" ", strip=True).lower()
    )
    
    if heading:
        print(f"Found heading: {heading.get_text(strip=True)}")
        container = heading.find_parent()
        table = container.find("table")
        if table:
            print(f"{'Tenor':<15} | {'Rate Raw':<20} | {'Volume Raw':<20}")
            print("-" * 60)
            for row in table.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) >= 3:
                    tenor = tds[0].get_text(" ", strip=True)
                    rate = tds[1].get_text(" ", strip=True)
                    vol = tds[2].get_text(" ", strip=True)
                    print(f"{tenor:<15} | {rate:<20} | {vol:<20}")
                elif len(tds) == 2:
                     tenor = tds[0].get_text(" ", strip=True)
                     rate = tds[1].get_text(" ", strip=True)
                     print(f"{tenor:<15} | {rate:<20} | {'(MISSING COL)':<20}")

        else:
            print("❌ Table not found")
    else:
        print("❌ Heading not found")

except Exception as e:
    print(f"Error: {e}")
