# configs/settings.py

import os
from dotenv import load_dotenv

load_dotenv()

# ===== Core Project Settings =====
PROJECT_ID = os.getenv("PROJECT_ID")

# ===== Buckets =====
RAW_BUCKET = "crypto-market-raw-data-us"

# ===== BigQuery Datasets =====
RAW_DATASET = "raw_market_data"
STAGING_DATASET = "staging_market_data"
CURATED_DATASET = "curated_market_data"
METADATA_DATASET = "platform_metadata"

# ===== Tables =====
RAW_TABLE = "crypto_api_responses"
STAGING_TABLE = "crypto_price_timeseries"
CURATED_TABLE = "crypto_daily_metrics"
METADATA_TABLE = "pipeline_run_metadata"

# ===== Coins (Portfolio Scope) =====
COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "ripple",
    "cardano"
]

# ===== Pipeline Modes =====
BACKFILL_DAYS = 90
INCREMENTAL_DAYS = 2   # sliding window