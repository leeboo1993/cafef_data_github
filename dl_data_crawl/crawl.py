#!/usr/bin/env python3
"""
DL Equity Incremental Data Crawler
Crawls data from https://www.dl-equity.com into a SQLite database.
Supports incremental updates — only fetches new data since last crawl.

Usage:
    python crawl_incremental.py init                        # Create DB with schema
    python crawl_incremental.py crawl                       # Incremental (since last crawl)
    python crawl_incremental.py crawl --full                # Full re-crawl (365 days)
    python crawl_incremental.py crawl --date 2026-02-27     # Crawl a specific day only
    python crawl_incremental.py crawl --from 2026-02-20 --to 2026-02-27  # Backfill a range
    python crawl_incremental.py crawl --skip-fundamentals   # Skip slow per-ticker data
    python crawl_incremental.py status                      # Show crawl metadata
    python crawl_incremental.py export                      # Export all tables to CSV
"""

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import requests

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

BASE_URL = "https://www.dl-equity.com"
LOGIN_URL = f"{BASE_URL}/login"
USERNAME = "Guest"
PASSWORD = "Guest123DL@3710"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "data", "db", "dl_equity.db")
EXPORT_DIR = os.path.join(SCRIPT_DIR, "data")

# ═══════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════

SCHEMA_SQL = """
-- ══════════════════════════════════════════════
-- TIME-SERIES TABLES
-- ══════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS vn_ta (
    date                   TEXT PRIMARY KEY,
    vnindex                REAL,
    vnindex_change_pct     REAL,
    rsi_21                 REAL,
    breadth_above_ma50     REAL,
    pct_outperform_vni_7d  REAL,
    foreign_net_value      REAL,
    foreign_buy_value      REAL,
    foreign_sell_value     REAL,
    dc_foreign_flow        REAL,
    advances               INTEGER,
    declines               INTEGER
);

CREATE TABLE IF NOT EXISTS sector_leadership (
    date       TEXT,
    sector     TEXT,
    net_score  REAL,
    ret_5d     REAL,
    PRIMARY KEY (date, sector)
);

CREATE TABLE IF NOT EXISTS derivatives_prop (
    date                   TEXT PRIMARY KEY,
    total_net_today        REAL,
    total_outstanding      REAL,
    outstanding_balance_bn REAL,
    vn30_close             REAL,
    f1_code                TEXT
);

CREATE TABLE IF NOT EXISTS dc_cash_ratio (
    date      TEXT PRIMARY KEY,
    veil      REAL,
    veil_net  REAL,
    tsk       REAL,
    tsk_net   REAL,
    dcds      REAL,
    dcds_net  REAL,
    nbim      REAL,
    nbim_net  REAL
);

CREATE TABLE IF NOT EXISTS economics_deposit (
    date   TEXT,
    bank   TEXT,
    tenor  TEXT,
    rate   REAL,
    PRIMARY KEY (date, bank, tenor)
);

CREATE TABLE IF NOT EXISTS economics_interbank (
    date   TEXT,
    tenor  TEXT,
    rate   REAL,
    PRIMARY KEY (date, tenor)
);

CREATE TABLE IF NOT EXISTS economics_treasury (
    date       TEXT,
    instrument TEXT,
    rate       REAL,
    PRIMARY KEY (date, instrument)
);

-- ══════════════════════════════════════════════
-- EVENT / FEED TABLES
-- ══════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ticker_news (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT,
    broker       TEXT,
    sentiment    TEXT,
    news_type    TEXT,
    snippet      TEXT,
    body         TEXT,
    date         TEXT,
    content_hash TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS macro_research (
    url     TEXT PRIMARY KEY,
    source  TEXT,
    title   TEXT,
    summary TEXT,
    date    TEXT
);

CREATE TABLE IF NOT EXISTS weekly_calls (
    week_ending  TEXT,
    ticker       TEXT,
    broker       TEXT,
    report_date  TEXT,
    shift        TEXT,
    PRIMARY KEY (week_ending, ticker, broker)
);

-- ══════════════════════════════════════════════
-- SNAPSHOT / REFERENCE TABLES
-- ══════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ticker_sector_map (
    ticker TEXT PRIMARY KEY,
    sector TEXT
);

CREATE TABLE IF NOT EXISTS sector_overview (
    sector       TEXT PRIMARY KEY,
    ticker_count INTEGER
);

CREATE TABLE IF NOT EXISTS put_through (
    ticker             TEXT,
    sector             TEXT,
    pt_volume          INTEGER,
    outstanding_shares INTEGER,
    pt_os_pct          REAL,
    last_price         REAL,
    period_days        INTEGER,
    crawl_date         TEXT,
    PRIMARY KEY (ticker, period_days)
);

-- ══════════════════════════════════════════════
-- PER-TICKER FUNDAMENTALS
-- ══════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS stock_info (
    ticker           TEXT PRIMARY KEY,
    sector_l1        TEXT,
    sector_l2        TEXT,
    sector_l3        TEXT,
    mcap_class       TEXT,
    mkt_cap          REAL,
    shares           REAL,
    earnings_score   REAL,
    earnings_quarter TEXT
);

CREATE TABLE IF NOT EXISTS quarterly_financials (
    ticker           TEXT,
    quarter          TEXT,
    revenue          REAL,
    revenue_ma4      REAL,
    gross_profit     REAL,
    gross_profit_ma4 REAL,
    ebit             REAL,
    ebit_ma4         REAL,
    npatmi           REAL,
    npatmi_ma4       REAL,
    PRIMARY KEY (ticker, quarter)
);

-- ══════════════════════════════════════════════
-- METADATA
-- ══════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS crawl_metadata (
    endpoint     TEXT PRIMARY KEY,
    last_crawl_at TEXT,
    last_date    TEXT,
    record_count INTEGER
);
"""

# ═══════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ═══════════════════════════════════════════════════════════════

def get_db() -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables."""
    conn = get_db()
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    print(f"✅ Database initialized: {DB_PATH}")


def get_last_date(conn: sqlite3.Connection, endpoint: str) -> str | None:
    """Get the last data date for an endpoint."""
    row = conn.execute(
        "SELECT last_date FROM crawl_metadata WHERE endpoint = ?",
        (endpoint,)
    ).fetchone()
    return row[0] if row else None


def update_metadata(conn: sqlite3.Connection, endpoint: str, last_date: str, count: int):
    """Update crawl metadata for an endpoint."""
    conn.execute(
        """INSERT OR REPLACE INTO crawl_metadata (endpoint, last_crawl_at, last_date, record_count)
           VALUES (?, ?, ?, ?)""",
        (endpoint, datetime.now().isoformat(), last_date, count)
    )


# ═══════════════════════════════════════════════════════════════
# NETWORK
# ═══════════════════════════════════════════════════════════════

def create_session() -> requests.Session:
    """Create an authenticated session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return session


def authenticate(session: requests.Session) -> bool:
    """Authenticate via POST /login."""
    print("🔐 Authenticating...")
    try:
        session.get(LOGIN_URL)
        resp = session.post(LOGIN_URL, data={"username": USERNAME, "password": PASSWORD}, allow_redirects=True)
        resp.raise_for_status()

        test = session.get(f"{BASE_URL}/api/sector-overview", timeout=10)
        if test.status_code == 200:
            print("  ✅ Authenticated successfully")
            return True
        print(f"  ❌ API test returned {test.status_code}")
        return False
    except Exception as e:
        print(f"  ❌ Login failed: {e}")
        return False


def fetch_api(session: requests.Session, endpoint: str, params: dict = None) -> dict | None:
    """Fetch JSON from an API endpoint."""
    url = f"{BASE_URL}{endpoint}"
    try:
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  ❌ Error fetching {endpoint}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# INGESTION FUNCTIONS (one per endpoint type)
# ═══════════════════════════════════════════════════════════════

def calc_days_needed(last_date: str | None, full: bool, target_date: str | None = None) -> int:
    """Calculate how many days to request.
    
    If target_date is set, fetch enough days to include that date.
    Otherwise, fetch since last_date (incremental) or 365 (full).
    """
    if target_date:
        target = datetime.strptime(target_date, "%Y-%m-%d")
        gap = (datetime.now() - target).days + 1
        return max(gap, 2)
    if full or not last_date:
        return 365
    last = datetime.strptime(last_date, "%Y-%m-%d")
    gap = (datetime.now() - last).days + 1  # +1 overlap for safety
    return max(gap, 2)


def filter_by_date(rows: list, target_date: str | None) -> list:
    """If target_date is set, keep only rows matching that date."""
    if not target_date:
        return rows
    return [r for r in rows if r.get("date") == target_date]


def ingest_vn_ta(session: requests.Session, conn: sqlite3.Connection, full: bool, target_date: str | None = None):
    """Ingest VN-Index technical analysis data."""
    endpoint = "vn_ta"
    last = get_last_date(conn, endpoint)
    days = calc_days_needed(last, full, target_date)
    label = f"date={target_date}" if target_date else f"days={days}"
    print(f"  📥 vn_ta ({label})...", end=" ", flush=True)

    data = fetch_api(session, f"/api/vn-ta?days={days}")
    if not data or "data" not in data:
        print("❌"); return

    rows = filter_by_date(data["data"], target_date)
    for r in rows:
        conn.execute(
            """INSERT OR REPLACE INTO vn_ta
               (date, vnindex, vnindex_change_pct, rsi_21, breadth_above_ma50,
                pct_outperform_vni_7d, foreign_net_value, foreign_buy_value,
                foreign_sell_value, dc_foreign_flow, advances, declines)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r.get("date"), r.get("vnindex"), r.get("vnindex_change_pct"),
             r.get("rsi_21"), r.get("breadth_above_ma50"),
             r.get("pct_outperform_vni_7d"), r.get("foreign_net_value"),
             r.get("foreign_buy_value"), r.get("foreign_sell_value"),
             r.get("dc_foreign_flow"), r.get("advances"), r.get("declines"))
        )

    latest = rows[-1]["date"] if rows else last
    update_metadata(conn, endpoint, latest, len(rows))
    conn.commit()
    print(f"✅ {len(rows)} rows")


def ingest_sector_leadership(session: requests.Session, conn: sqlite3.Connection, full: bool, target_date: str | None = None):
    """Ingest sector leadership data (nested sectors per day)."""
    endpoint = "sector_leadership"
    last = get_last_date(conn, endpoint)
    days = calc_days_needed(last, full, target_date)
    label = f"date={target_date}" if target_date else f"days={days}"
    print(f"  📥 sector_leadership ({label})...", end=" ", flush=True)

    data = fetch_api(session, f"/api/sector-leadership?days={days}")
    if not data or "data" not in data:
        print("❌"); return

    filtered = data["data"]
    if target_date:
        filtered = [d for d in filtered if d["date"] == target_date]

    count = 0
    latest_date = last
    for day in filtered:
        d = day["date"]
        sectors = day.get("sectors", {})
        for sector_name, vals in sectors.items():
            conn.execute(
                """INSERT OR REPLACE INTO sector_leadership (date, sector, net_score, ret_5d)
                   VALUES (?,?,?,?)""",
                (d, sector_name, vals.get("net"), vals.get("ret5d"))
            )
            count += 1
        if d > (latest_date or ""):
            latest_date = d

    update_metadata(conn, endpoint, latest_date, count)
    conn.commit()
    print(f"✅ {count} rows ({len(data['data'])} days)")


def ingest_derivatives_prop(session: requests.Session, conn: sqlite3.Connection, full: bool, target_date: str | None = None):
    """Ingest derivatives proprietary trading data."""
    endpoint = "derivatives_prop"
    last = get_last_date(conn, endpoint)
    days = calc_days_needed(last, full, target_date)
    label = f"date={target_date}" if target_date else f"days={days}"
    print(f"  📥 derivatives_prop ({label})...", end=" ", flush=True)

    data = fetch_api(session, f"/api/derivatives-prop?days={days}")
    if not data or "data" not in data:
        print("❌"); return

    rows = filter_by_date(data["data"], target_date)
    for r in rows:
        conn.execute(
            """INSERT OR REPLACE INTO derivatives_prop
               (date, total_net_today, total_outstanding, outstanding_balance_bn, vn30_close, f1_code)
               VALUES (?,?,?,?,?,?)""",
            (r.get("date"), r.get("total_net_today"), r.get("total_outstanding"),
             r.get("outstanding_balance_bn"), r.get("vn30_close"), r.get("f1_code"))
        )

    latest = rows[-1]["date"] if rows else last
    update_metadata(conn, endpoint, latest, len(rows))
    conn.commit()
    print(f"✅ {len(rows)} rows")


def ingest_dc_cash_ratio(session: requests.Session, conn: sqlite3.Connection, full: bool, target_date: str | None = None):
    """Ingest DC fund cash ratios."""
    endpoint = "dc_cash_ratio"
    last = get_last_date(conn, endpoint)
    days = calc_days_needed(last, full, target_date)
    label = f"date={target_date}" if target_date else f"days={days}"
    print(f"  📥 dc_cash_ratio ({label})...", end=" ", flush=True)

    data = fetch_api(session, f"/api/dc-cash-ratio?days={days}")
    if not data or "data" not in data:
        print("❌"); return

    rows = filter_by_date(data["data"], target_date)
    for r in rows:
        conn.execute(
            """INSERT OR REPLACE INTO dc_cash_ratio
               (date, veil, veil_net, tsk, tsk_net, dcds, dcds_net, nbim, nbim_net)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (r.get("date"), r.get("VEIL"), r.get("VEIL_net"),
             r.get("TSK"), r.get("TSK_net"), r.get("DCDS"), r.get("DCDS_net"),
             r.get("NBIM"), r.get("NBIM_net"))
        )

    latest = rows[-1]["date"] if rows else last
    update_metadata(conn, endpoint, latest, len(rows))
    conn.commit()
    print(f"✅ {len(rows)} rows")


def ingest_economics(session: requests.Session, conn: sqlite3.Connection, full: bool, target_date: str | None = None):
    """Ingest economics data (deposit, interbank, treasury)."""
    endpoint = "economics_data"
    last = get_last_date(conn, endpoint)
    days = calc_days_needed(last, full, target_date)
    label = f"date={target_date}" if target_date else f"days={days}"
    print(f"  📥 economics_data ({label})...", end=" ", flush=True)

    data = fetch_api(session, f"/api/economics-data?days={days}")
    if not data:
        print("❌"); return

    total = 0
    latest_date = last

    # Deposit rates
    if "deposit" in data:
        dep = data["deposit"]
        dates = dep.get("dates", [])
        for bank, tenors in dep.items():
            if bank == "dates" or not isinstance(tenors, dict):
                continue
            for tenor, rates in tenors.items():
                for i, rate in enumerate(rates):
                    if i < len(dates) and rate is not None:
                        if target_date and dates[i] != target_date:
                            continue
                        conn.execute(
                            "INSERT OR REPLACE INTO economics_deposit (date, bank, tenor, rate) VALUES (?,?,?,?)",
                            (dates[i], bank, tenor, rate)
                        )
                        total += 1
        if dates:
            latest_date = max(dates[-1], latest_date or "")

    # Interbank rates
    if "interbank" in data:
        ib = data["interbank"]
        dates = ib.get("dates", [])
        for tenor, rates in ib.items():
            if tenor == "dates" or not isinstance(rates, list):
                continue
            for i, rate in enumerate(rates):
                if i < len(dates) and rate is not None:
                    if target_date and dates[i] != target_date:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO economics_interbank (date, tenor, rate) VALUES (?,?,?)",
                        (dates[i], tenor, rate)
                    )
                    total += 1

    # Treasury rates
    if "treasury" in data:
        tr = data["treasury"]
        dates = tr.get("dates", [])
        for instrument, rates in tr.items():
            if instrument == "dates" or not isinstance(rates, list):
                continue
            for i, rate in enumerate(rates):
                if i < len(dates) and rate is not None:
                    if target_date and dates[i] != target_date:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO economics_treasury (date, instrument, rate) VALUES (?,?,?)",
                        (dates[i], instrument, rate)
                    )
                    total += 1

    update_metadata(conn, endpoint, target_date or latest_date, total)
    conn.commit()
    print(f"✅ {total} rows")


def ingest_ticker_news(session: requests.Session, conn: sqlite3.Connection, full: bool):
    """Ingest ticker news with deduplication."""
    endpoint = "ticker_news"
    print(f"  📥 ticker_news...", end=" ", flush=True)

    data = fetch_api(session, "/api/ticker-news")
    if not data or "news" not in data:
        print("❌"); return

    inserted = 0
    for item in data["news"]:
        # Create content hash for dedup
        raw = f"{item.get('ticker')}|{item.get('date')}|{item.get('snippet', '')[:100]}"
        content_hash = hashlib.sha256(raw.encode()).hexdigest()

        try:
            conn.execute(
                """INSERT OR IGNORE INTO ticker_news
                   (ticker, broker, sentiment, news_type, snippet, body, date, content_hash)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (item.get("ticker"), item.get("broker"), item.get("sentiment"),
                 item.get("news_type"), item.get("snippet"), item.get("body"),
                 item.get("date"), content_hash)
            )
            if conn.total_changes:
                inserted += 1
        except sqlite3.IntegrityError:
            pass  # Duplicate, skip

    latest = data["news"][0]["date"] if data["news"] else None
    update_metadata(conn, endpoint, latest, len(data["news"]))
    conn.commit()
    print(f"✅ {len(data['news'])} fetched, {inserted} new")


def ingest_macro_research(session: requests.Session, conn: sqlite3.Connection, full: bool):
    """Ingest macro research articles with dedup by URL."""
    endpoint = "macro_research"
    print(f"  📥 macro_research...", end=" ", flush=True)

    data = fetch_api(session, "/api/macro-research?days=90&limit=100")
    if not data or "articles" not in data:
        print("❌"); return

    inserted = 0
    for a in data["articles"]:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO macro_research (url, source, title, summary, date) VALUES (?,?,?,?,?)",
                (a.get("url"), a.get("source"), a.get("title"), a.get("summary"), a.get("date"))
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    latest = data["articles"][0]["date"] if data["articles"] else None
    update_metadata(conn, endpoint, latest, len(data["articles"]))
    conn.commit()
    print(f"✅ {len(data['articles'])} fetched, {inserted} new")


def ingest_weekly_calls(session: requests.Session, conn: sqlite3.Connection, full: bool):
    """Ingest weekly calls analysis."""
    endpoint = "weekly_calls"
    print(f"  📥 weekly_calls...", end=" ", flush=True)

    data = fetch_api(session, "/api/weekly-calls/latest")
    if not data or "reports_analyzed" not in data:
        print("❌"); return

    week = data.get("week_ending", "")
    count = 0
    for r in data["reports_analyzed"]:
        conn.execute(
            """INSERT OR REPLACE INTO weekly_calls
               (week_ending, ticker, broker, report_date, shift)
               VALUES (?,?,?,?,?)""",
            (week, r.get("ticker"), r.get("broker"), r.get("report_date"), r.get("shift"))
        )
        count += 1

    update_metadata(conn, endpoint, week, count)
    conn.commit()
    print(f"✅ {count} rows (week {week})")


def ingest_ticker_sector_map(session: requests.Session, conn: sqlite3.Connection, full: bool):
    """Ingest ticker-to-sector mapping (full replace)."""
    endpoint = "ticker_sector_map"
    print(f"  📥 ticker_sector_map...", end=" ", flush=True)

    data = fetch_api(session, "/api/ticker-sector-map")
    if not data:
        print("❌"); return

    conn.execute("DELETE FROM ticker_sector_map")
    for ticker, sector in data.items():
        conn.execute(
            "INSERT INTO ticker_sector_map (ticker, sector) VALUES (?,?)",
            (ticker, sector)
        )

    update_metadata(conn, endpoint, datetime.now().strftime("%Y-%m-%d"), len(data))
    conn.commit()
    print(f"✅ {len(data)} tickers")
    return data  # Return for fundamentals


def ingest_sector_overview(session: requests.Session, conn: sqlite3.Connection, full: bool):
    """Ingest sector overview (full replace)."""
    endpoint = "sector_overview"
    print(f"  📥 sector_overview...", end=" ", flush=True)

    data = fetch_api(session, "/api/sector-overview")
    if not data or "sectors" not in data:
        print("❌"); return

    conn.execute("DELETE FROM sector_overview")
    for s in data["sectors"]:
        conn.execute(
            "INSERT INTO sector_overview (sector, ticker_count) VALUES (?,?)",
            (s["sector"], s["count"])
        )

    update_metadata(conn, endpoint, datetime.now().strftime("%Y-%m-%d"), len(data["sectors"]))
    conn.commit()
    print(f"✅ {len(data['sectors'])} sectors")


def ingest_put_through(session: requests.Session, conn: sqlite3.Connection, full: bool):
    """Ingest put-through data for 1D, 7D, 30D (snapshot replace)."""
    endpoint = "put_through"
    print(f"  📥 put_through (1D/7D/30D)...", end=" ", flush=True)

    total = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for period in [1, 7, 30]:
        data = fetch_api(session, f"/api/put-through?days={period}")
        if not data or "data" not in data:
            continue

        conn.execute("DELETE FROM put_through WHERE period_days = ?", (period,))
        for r in data["data"]:
            conn.execute(
                """INSERT INTO put_through
                   (ticker, sector, pt_volume, outstanding_shares, pt_os_pct, last_price, period_days, crawl_date)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (r.get("ticker"), r.get("sector"), r.get("pt_volume"),
                 r.get("outstanding_shares"), r.get("pt_os_pct"),
                 r.get("last_price"), period, today)
            )
            total += 1

    update_metadata(conn, endpoint, today, total)
    conn.commit()
    print(f"✅ {total} rows across 3 periods")


def ingest_fundamentals(session: requests.Session, conn: sqlite3.Connection, full: bool, tickers: list):
    """Ingest per-ticker company financials."""
    endpoint = "fundamentals"
    last_crawl = get_last_date(conn, endpoint)

    # Skip if already crawled today (unless --full)
    today = datetime.now().strftime("%Y-%m-%d")
    if not full and last_crawl == today:
        print(f"  📥 fundamentals... ⏭️  Already crawled today")
        return

    total = len(tickers)
    success = 0
    failed = []
    print(f"\n  📊 Crawling fundamentals for {total} tickers...")

    for i, ticker in enumerate(tickers, 1):
        data = fetch_api(session, f"/api/company-financials/{ticker}?quarters=8")
        if not data:
            failed.append(ticker)
            continue

        # Stock info
        info = data.get("stock_info", {})
        if info:
            conn.execute(
                """INSERT OR REPLACE INTO stock_info
                   (ticker, sector_l1, sector_l2, sector_l3, mcap_class, mkt_cap, shares, earnings_score, earnings_quarter)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (ticker, info.get("sector_l1"), info.get("sector_l2"), info.get("sector_l3"),
                 info.get("mcap_class"), info.get("mkt_cap"), info.get("shares"),
                 info.get("earnings_score"), info.get("earnings_quarter"))
            )

        # Quarterly financials
        for q in data.get("quarters", []):
            conn.execute(
                """INSERT OR REPLACE INTO quarterly_financials
                   (ticker, quarter, revenue, revenue_ma4, gross_profit, gross_profit_ma4,
                    ebit, ebit_ma4, npatmi, npatmi_ma4)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (ticker, q.get("quarter"), q.get("revenue"), q.get("revenue_ma4"),
                 q.get("gross_profit"), q.get("gross_profit_ma4"),
                 q.get("ebit"), q.get("ebit_ma4"),
                 q.get("npatmi"), q.get("npatmi_ma4"))
            )

        success += 1

        if i % 50 == 0 or i == total:
            conn.commit()
            print(f"     📈 Progress: {i}/{total} ({success} ok, {len(failed)} failed)")

        time.sleep(0.1)  # Be respectful

    update_metadata(conn, endpoint, today, success)
    conn.commit()
    print(f"  ✅ Fundamentals: {success}/{total} tickers")
    if failed:
        print(f"  ⚠️  Failed: {', '.join(failed[:20])}" +
              (f" (+{len(failed)-20} more)" if len(failed) > 20 else ""))


# ═══════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════

def cmd_init():
    """Initialize the database."""
    init_db()


def _crawl_one_day(session: requests.Session, conn: sqlite3.Connection,
                   full: bool, skip_fundamentals: bool, target_date: str | None = None,
                   is_range: bool = False):
    """Crawl data for a single target_date (or incremental if None).
    
    The date filter only applies to time-series tables (rows keyed by date).
    Events, snapshots, and fundamentals are always crawled (they represent
    current/latest state, not date-specific data).
    When is_range=True (part of a --from/--to loop), non-time-series data
    is only crawled on the first call to avoid redundant refetches.
    """

    # Time-series (incremental or date-filtered)
    ingest_vn_ta(session, conn, full, target_date)
    ingest_sector_leadership(session, conn, full, target_date)
    ingest_derivatives_prop(session, conn, full, target_date)
    ingest_dc_cash_ratio(session, conn, full, target_date)
    ingest_economics(session, conn, full, target_date)

    # Skip non-time-series data when inside a range loop (already fetched on first day)
    if is_range:
        return

    # Events/feeds (dedup) — always crawl
    ingest_ticker_news(session, conn, full)
    ingest_macro_research(session, conn, full)
    ingest_weekly_calls(session, conn, full)

    # Snapshots (always replace)
    ticker_map = ingest_ticker_sector_map(session, conn, full)
    ingest_sector_overview(session, conn, full)
    ingest_put_through(session, conn, full)

    # Per-ticker fundamentals
    if skip_fundamentals:
        print(f"  ⏭️  Skipping fundamentals")
    else:
        if not ticker_map:
            ticker_map = fetch_api(session, "/api/ticker-sector-map")
        if ticker_map:
            tickers = sorted(ticker_map.keys())
            ingest_fundamentals(session, conn, full, tickers)


def cmd_crawl(full: bool = False, skip_fundamentals: bool = False,
              target_date: str | None = None,
              date_from: str | None = None, date_to: str | None = None):
    """Run the crawler."""

    # Determine mode label
    if target_date:
        mode = f"SINGLE-DAY ({target_date})"
    elif date_from and date_to:
        mode = f"RANGE ({date_from} → {date_to})"
    elif full:
        mode = "FULL"
    else:
        mode = "INCREMENTAL"

    print(f"\n{'='*60}")
    print(f"  DL Equity {mode} Crawl")
    print(f"{'='*60}")

    # Init DB if needed
    init_db()

    # Authenticate
    session = create_session()
    if not authenticate(session):
        print("❌ Authentication failed.")
        sys.exit(1)

    conn = get_db()

    # Date-range crawl: loop day-by-day
    if date_from and date_to:
        start = datetime.strptime(date_from, "%Y-%m-%d")
        end = datetime.strptime(date_to, "%Y-%m-%d")
        if start > end:
            print("❌ --from date must be before --to date")
            sys.exit(1)

        current = start
        day_count = (end - start).days + 1
        print(f"\n🌐 Crawling {day_count} days ({date_from} → {date_to})...\n")

        # First: crawl non-time-series data once (news, snapshots, fundamentals)
        print(f"\n📦 Crawling latest snapshots, news & fundamentals...")
        _crawl_one_day(session, conn, full, skip_fundamentals, date_from, is_range=False)

        # Then: loop remaining days for time-series only
        current = start + timedelta(days=1)
        while current <= end:
            day_str = current.strftime("%Y-%m-%d")
            print(f"\n📅 ── {day_str} ──")
            _crawl_one_day(session, conn, full, skip_fundamentals, day_str, is_range=True)
            current += timedelta(days=1)

    # Single-date crawl
    elif target_date:
        print(f"\n🌐 Crawling data for {target_date}...\n")
        _crawl_one_day(session, conn, full, skip_fundamentals, target_date)

    # Normal incremental / full crawl
    else:
        print(f"\n🌐 Crawling data...\n")
        _crawl_one_day(session, conn, full, skip_fundamentals, None)

    conn.close()

    print(f"\n{'='*60}")
    print(f"  ✅ Crawl Complete! Database: {DB_PATH}")
    print(f"{'='*60}\n")


def cmd_status():
    """Show crawl metadata."""
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Run 'init' first.")
        return

    conn = get_db()

    print(f"\n{'='*60}")
    print(f"  Crawl Status — {DB_PATH}")
    print(f"{'='*60}\n")

    # Metadata table
    rows = conn.execute(
        "SELECT endpoint, last_crawl_at, last_date, record_count FROM crawl_metadata ORDER BY endpoint"
    ).fetchall()

    if not rows:
        print("  No crawl history found. Run 'crawl' first.")
        conn.close()
        return

    print(f"  {'Endpoint':<25} {'Last Crawl':<22} {'Data Date':<12} {'Records':>8}")
    print(f"  {'─'*25} {'─'*22} {'─'*12} {'─'*8}")
    for r in rows:
        crawl_time = r[1][:19] if r[1] else "—"
        print(f"  {r[0]:<25} {crawl_time:<22} {r[2] or '—':<12} {r[3] or 0:>8}")

    # Table sizes
    print(f"\n  {'Table':<25} {'Rows':>10}")
    print(f"  {'─'*25} {'─'*10}")
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name != 'crawl_metadata' ORDER BY name"
    ).fetchall()
    for (tbl,) in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM [{tbl}]").fetchone()[0]
        print(f"  {tbl:<25} {cnt:>10}")

    # DB file size
    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"\n  📁 Database size: {size_mb:.1f} MB")

    conn.close()


def cmd_export(fmt: str = "all"):
    """Export all tables to CSV and/or JSON."""
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Run 'init' first.")
        return

    do_csv = fmt in ("csv", "all")
    do_json = fmt in ("json", "all")

    conn = get_db()
    conn.row_factory = sqlite3.Row

    def table_to_list(table: str) -> list:
        return [dict(r) for r in conn.execute(f"SELECT * FROM [{table}]").fetchall()]

    def table_count(table: str) -> int:
        return conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]

    # ── CSV export (one file per table) ──
    if do_csv:
        csv_dir = os.path.join(EXPORT_DIR, "csv")
        os.makedirs(csv_dir, exist_ok=True)
        print(f"\n📤 Exporting CSV to {csv_dir}\n")

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('sqlite_sequence') ORDER BY name"
        ).fetchall()

        for (tbl,) in tables:
            cursor = conn.execute(f"SELECT * FROM [{tbl}]")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            filepath = os.path.join(csv_dir, f"{tbl}.csv")
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)

            print(f"  ✅ {tbl}.csv ({len(rows)} rows)")

    # ── JSON export (4 consolidated files) ──
    if do_json:
        json_dir = os.path.join(EXPORT_DIR, "json")
        os.makedirs(json_dir, exist_ok=True)
        print(f"\n📤 Exporting JSON to {json_dir}\n")

        # 1) market.json — all market time-series
        market = {
            "vn_index":           table_to_list("vn_ta"),
            "sector_leadership":  table_to_list("sector_leadership"),
            "derivatives_prop":   table_to_list("derivatives_prop"),
            "dc_cash_ratio":      table_to_list("dc_cash_ratio"),
            "put_through":        table_to_list("put_through"),
        }
        with open(os.path.join(json_dir, "market.json"), "w", encoding="utf-8") as f:
            json.dump(market, f, ensure_ascii=False, indent=2)
        counts = {k: len(v) for k, v in market.items()}
        print(f"  ✅ market.json ({sum(counts.values())} records: {counts})")

        # 2) economics.json — rates data
        economics = {
            "deposit":   table_to_list("economics_deposit"),
            "interbank": table_to_list("economics_interbank"),
            "treasury":  table_to_list("economics_treasury"),
        }
        with open(os.path.join(json_dir, "economics.json"), "w", encoding="utf-8") as f:
            json.dump(economics, f, ensure_ascii=False, indent=2)
        counts = {k: len(v) for k, v in economics.items()}
        print(f"  ✅ economics.json ({sum(counts.values())} records: {counts})")

        # 3) research.json — news, macro articles, weekly calls
        research = {
            "ticker_news":    table_to_list("ticker_news"),
            "macro_research": table_to_list("macro_research"),
            "weekly_calls":   table_to_list("weekly_calls"),
        }
        with open(os.path.join(json_dir, "research.json"), "w", encoding="utf-8") as f:
            json.dump(research, f, ensure_ascii=False, indent=2)
        counts = {k: len(v) for k, v in research.items()}
        print(f"  ✅ research.json ({sum(counts.values())} records: {counts})")

        # 4) fundamentals.json — per-ticker data, nested by ticker
        stock_info = table_to_list("stock_info")
        financials = table_to_list("quarterly_financials")
        sector_map_rows = table_to_list("ticker_sector_map")

        # Build ticker → sector lookup
        sector_map = {r["ticker"]: r["sector"] for r in sector_map_rows}

        # Build ticker → financials lookup
        fin_by_ticker = {}
        for f_row in financials:
            t = f_row["ticker"]
            if t not in fin_by_ticker:
                fin_by_ticker[t] = []
            fin_copy = {k: v for k, v in f_row.items() if k != "ticker"}
            fin_by_ticker[t].append(fin_copy)

        # Merge into one object per ticker
        tickers = {}
        for s in stock_info:
            t = s["ticker"]
            tickers[t] = {
                "sector":     sector_map.get(t),
                "sector_l1":  s.get("sector_l1"),
                "sector_l2":  s.get("sector_l2"),
                "sector_l3":  s.get("sector_l3"),
                "mcap_class": s.get("mcap_class"),
                "mkt_cap":    s.get("mkt_cap"),
                "shares":     s.get("shares"),
                "earnings_score":   s.get("earnings_score"),
                "earnings_quarter": s.get("earnings_quarter"),
                "quarterly_financials": fin_by_ticker.get(t, []),
            }

        fundamentals = {
            "sector_overview": table_to_list("sector_overview"),
            "tickers": tickers,
        }
        with open(os.path.join(json_dir, "fundamentals.json"), "w", encoding="utf-8") as f:
            json.dump(fundamentals, f, ensure_ascii=False, indent=2)
        print(f"  ✅ fundamentals.json ({len(tickers)} tickers, {len(financials)} quarters)")

    conn.close()
    print(f"\n✅ Export complete! → {EXPORT_DIR}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="DL Equity Incremental Data Crawler")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    subparsers.add_parser("init", help="Initialize the database with schema")

    crawl_parser = subparsers.add_parser("crawl", help="Crawl data (incremental by default)")
    crawl_parser.add_argument("--full", action="store_true", help="Full re-crawl (365 days)")
    crawl_parser.add_argument("--date", type=str, default=None,
                              help="Crawl data for a specific date (YYYY-MM-DD)")
    crawl_parser.add_argument("--from", dest="date_from", type=str, default=None,
                              help="Start date for range crawl (YYYY-MM-DD)")
    crawl_parser.add_argument("--to", dest="date_to", type=str, default=None,
                              help="End date for range crawl (YYYY-MM-DD)")
    crawl_parser.add_argument("--skip-fundamentals", action="store_true",
                              help="Skip per-ticker fundamentals (faster daily runs)")

    subparsers.add_parser("status", help="Show crawl status and record counts")
    export_parser = subparsers.add_parser("export", help="Export all tables to CSV/JSON")
    export_parser.add_argument("--format", choices=["csv", "json", "all"], default="all",
                               help="Export format (default: all)")

    args = parser.parse_args()

    if args.command == "init":
        cmd_init()
    elif args.command == "crawl":
        cmd_crawl(full=args.full, skip_fundamentals=args.skip_fundamentals,
                  target_date=args.date, date_from=args.date_from, date_to=args.date_to)
    elif args.command == "status":
        cmd_status()
    elif args.command == "export":
        cmd_export(fmt=args.format)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
