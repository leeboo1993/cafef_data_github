
import re

def parse_volume(value: str):
    if not value or value.strip() in ['', '-', 'N/A', 'NA']:
        return None
    value = value.strip()
    tokens = value.split()
    for token in tokens:
        token_clean = re.sub(r'[*()]+', '', token).strip()
        if not token_clean: continue
        try:
            parts = re.split(r'[.,]', token_clean)
            parts = [p for p in parts if p]
            if not parts: continue
            if len(parts) == 1:
                return float(parts[0])
            last_part = parts[-1]
            if len(last_part) == 3:
                full_num_str = "".join(parts)
                return float(full_num_str)
            else:
                int_part = "".join(parts[:-1])
                dec_part = last_part
                return float(f"{int_part}.{dec_part}")
        except ValueError:
            continue
    return None

cases = [
    "567,855,0",
    "567.855,0",
    "567855,0",
    "11.615,0",
    "2.920,0",
    "1.000",   # 1000
    "6,45",    # 6.45
    "(*) 567,855,0",
    "567,855,0 (Some text)"
]

print(f"{'Input':<25} | {'Result':<15}")
print("-" * 45)
for c in cases:
    print(f"{c:<25} | {str(parse_volume(c)):<15}")
