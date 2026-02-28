# DL Equity Data Crawler

Incremental data crawler for [dl-equity.com](https://www.dl-equity.com) — Vietnamese stock market data including VN-Index technicals, sector analysis, economics, news, and per-ticker fundamentals.

## Project Structure

```
dl_data_crawl/
├── crawl.py              # Crawler script (single file)
├── README.md
└── data/
    ├── db/
    │   └── dl_equity.db  # SQLite database (source of truth)
    └── json/
        ├── market.json         # VN-Index, sectors, derivatives, cash ratios
        ├── economics.json      # Deposit, interbank, treasury rates
        ├── research.json       # Ticker news, macro articles, weekly calls
        └── fundamentals.json   # 534 tickers with quarterly financials
```

## Setup

```bash
pip install requests
```

## Quick Start

```bash
# First run — full crawl (downloads 365 days + 534 tickers, ~3 min)
python3 crawl.py crawl --full

# Export to JSON
python3 crawl.py export --format json
```

## Commands

### `crawl` — Download Data

```bash
# Incremental crawl (only new data since last crawl)
python3 crawl.py crawl

# Specific date
python3 crawl.py crawl --date 2026-02-25

# Date range (backfill)
python3 crawl.py crawl --from 2026-02-20 --to 2026-02-27

# Full re-download (365 days)
python3 crawl.py crawl --full

# Skip fundamentals for faster daily runs (~10s instead of ~3min)
python3 crawl.py crawl --skip-fundamentals
```

**Flags can be combined:**
```bash
python3 crawl.py crawl --date 2026-02-25 --skip-fundamentals
```

### `status` — Check Database

```bash
python3 crawl.py status
```

Shows last crawl time, latest data date, and row counts per table.

### `export` — Export Data

```bash
python3 crawl.py export                  # Both CSV + JSON
python3 crawl.py export --format json    # JSON only
python3 crawl.py export --format csv     # CSV only
```

### `init` — Initialize Database

```bash
python3 crawl.py init
```

Creates the SQLite database with schema. **Not required** — `crawl` auto-initializes.

## Recommended Daily Workflow

```bash
# 1. Crawl new data (runs in ~10 seconds)
python3 crawl.py crawl --skip-fundamentals

# 2. Re-export JSON
python3 crawl.py export --format json

# 3. (Weekly) Also update fundamentals
python3 crawl.py crawl
```

## Database Schema

### Time-Series (date-keyed, grows daily)

| Table | Description | Key |
|---|---|---|
| `vn_ta` | VN-Index, RSI, breadth, foreign flow | `date` |
| `sector_leadership` | Per-sector lead/lag scores | `date, sector` |
| `derivatives_prop` | Derivatives proprietary trading | `date` |
| `dc_cash_ratio` | Fund cash ratios (VEIL, TSK, DCDS, NBIM) | `date` |
| `economics_deposit` | Bank deposit rates by tenor | `date, bank, tenor` |
| `economics_interbank` | Interbank rates | `date, tenor` |
| `economics_treasury` | Treasury/OMO rates | `date, instrument` |

### Events (deduplicated, grows over time)

| Table | Description | Key |
|---|---|---|
| `ticker_news` | Analyst calls with sentiment | `content_hash` |
| `macro_research` | Macro research articles | `url` |
| `weekly_calls` | Weekly analyst call shifts | `week_ending, ticker, broker` |

### Snapshots (replaced each crawl)

| Table | Description | Key |
|---|---|---|
| `ticker_sector_map` | Ticker → sector mapping | `ticker` |
| `sector_overview` | Sector summary with counts | `sector` |
| `put_through` | Put-through transactions (1D/7D/30D) | `ticker, period_days` |

### Per-Ticker Fundamentals (quarterly data)

| Table | Description | Key |
|---|---|---|
| `stock_info` | Company info, mcap, earnings score | `ticker` |
| `quarterly_financials` | Revenue, gross profit, EBIT, NPATMI | `ticker, quarter` |

## JSON Export Structure

```
market.json
├── vn_index[]           — daily VN-Index data
├── sector_leadership[]  — daily sector scores
├── derivatives_prop[]   — derivatives trading
├── dc_cash_ratio[]      — fund cash ratios
└── put_through[]        — put-through transactions

economics.json
├── deposit[]    — bank deposit rates
├── interbank[]  — interbank rates
└── treasury[]   — treasury rates

research.json
├── ticker_news[]     — analyst news with sentiment
├── macro_research[]  — macro articles
└── weekly_calls[]    — weekly call shifts

fundamentals.json
├── sector_overview[]  — sector summary
└── tickers{}          — keyed by ticker symbol
    └── {ticker}
        ├── sector, mcap_class, mkt_cap, shares, ...
        └── quarterly_financials[]
            └── {quarter, revenue, gross_profit, ebit, npatmi, ...}
```

## Incremental Logic

| Data Type | Strategy | Example |
|---|---|---|
| **Time-series** | Fetch only days since last crawl | `days=2` next day |
| **Events** | Deduplicate by hash/URL | Skips existing news |
| **Snapshots** | Always replace | Gets current state |
| **Fundamentals** | Skip if crawled today | Once per day max |
| **`--date` mode** | Filter to specific date | Stores 1 row per table |

## Authentication

Uses hardcoded guest credentials. The script authenticates via `POST /login` and maintains a session cookie for API access.

## Data Source

All data comes from [dl-equity.com](https://www.dl-equity.com)'s internal JSON APIs (14 endpoints + 534 per-ticker endpoints).
