#!/bin/bash
set -e
echo "🚀 Running USD Black Market scraper (T-5 to today)..."
python usd_black_market.py

echo "🚀 Running Trading Account scraper (last 6 months)..."
python trading_account.py

echo "🚀 Running Interbank Rate scraper..."
python on_rate_scrape.py

echo "🚀 Running Comprehensive Scraper (T-5 Business Days)..."
python cafef_comprehensive_scraper.py --lookback-days 5 --workers 10

echo "🚀 Running Global Market Scraper..."
python global_market_scraper.py

echo "🚀 Running ETF Flow Scraper..."
python etf_flow_scraper.py

echo "🚀 Running Investor Flow Scraper..."
python investor_flow_scraper.py

echo "₿ Running Crypto Scraper..."
python crypto_scraper.py

echo "📉 Running Bond Market Scraper..."
python bond_market_scraper.py

echo "🧹 Cleaning up interrupted/stuck uploads..."
python run_cleanup.py

echo "🎯 All done!"
