#!/bin/bash
# ==============================================================================
# 🚀 VNINDEX PREDICTION PIPELINE (OPTIMIZED V4.1)
# ==============================================================================

# 1. ENVIRONMENT SETUP
source /opt/anaconda3/bin/activate base
cd /Users/leeboo/Desktop/projects/new_model

# 2. DATA SYNC (INCREMENTAL)
echo "--------------------------------------------------------"
echo "📥 STEP 1: SYNCING LATEST MARKET DATA..."
echo "--------------------------------------------------------"

# A. Stock Prices (Check if update needed?)
# For now, we assume user/scheduler ran `daily_update_data.yml` or similar. 
# But let's run the Proprietary/Foreign sync explicitly as requested.
# Using defaults: checks existing file, re-downloads last 5 days to be safe.
/opt/anaconda3/bin/python3 scripts/sync/download_investor_flows.py

# 3. FEATURE ENGINEERING
echo "--------------------------------------------------------"
echo "🧠 STEP 2: UPDATING MASTER FEATURES..."
echo "--------------------------------------------------------"
# This reads the updated 'foreign_proprietary_flows.parquet' and 'all_order_statistics'
# and appends new rows to 'master_features.parquet'.
# Parse arguments to get the day
TARGET_DAY=""
for arg in "$@"; do
    if [[ "$arg" == --day=* ]]; then
        TARGET_DAY="${arg#*=}"
    elif [[ "$arg" == "--day" ]]; then
        # This simple parser assumes the next arg is the date, but for now we rely on the python script to handle it cleanly via $@
        # However, to pass it explicitly to other scripts we might need to extract it.
        # Let's rely on passing "$@" to the python scripts that support it, but patch_missing_data needs it.
        :
    fi
done

# We use a simple trick: pass "$@" to patch_missing_data.py
# 2.5 PATCH MISSING DATA (VNSTOCK FALLBACK)
echo "--------------------------------------------------------"
echo "🛠️ STEP 1.5: PATCHING MISSING PRICE DATA..."
echo "--------------------------------------------------------"
/opt/anaconda3/bin/python3 scripts/sync/patch_missing_data.py "$@"

# If we patched data, we MUST regenerate the regime features
# To be safe, we run this chain every time we patch/sync
echo "--------------------------------------------------------"
echo "🔄 STEP 1.6: REGENERATING REGIME FEATURES..."
echo "--------------------------------------------------------"
/opt/anaconda3/bin/python3 scripts/research/detect_regimes.py
/opt/anaconda3/bin/python3 scripts/research/compare_window_sizes.py
/opt/anaconda3/bin/python3 scripts/production/regime_detection.py
/opt/anaconda3/bin/python3 scripts/research/regime_crossover_analysis.py

/opt/anaconda3/bin/python3 scripts/production/create_master_features.py

# 4. INFERENCE (Model V4)
echo "--------------------------------------------------------"
echo "🔮 STEP 3: GENERATING FORECAST (Mixture of Experts)..."
echo "--------------------------------------------------------"
# Runs the classification (Gate) + Expert Models + Retail Reference
/opt/anaconda3/bin/python3 scripts/production/show_forecast.py "$@"

echo "--------------------------------------------------------"
echo "✅ PIPELINE COMPLETE."
echo "--------------------------------------------------------"
