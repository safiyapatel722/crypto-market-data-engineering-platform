# configs/settings.py

import os
from dotenv import load_dotenv

load_dotenv()

# ── Core Project Settings ──────────────────────────────────────────────────────
# FIX: Validate required env vars at startup so you get a clear error message
# immediately, rather than a cryptic "NoneType" crash deep inside a GCP call.

PROJECT_ID = os.getenv("PROJECT_ID")
if not PROJECT_ID:
    raise EnvironmentError(
        "Required environment variable 'PROJECT_ID' is not set. "
        "Add it to your .env file or Cloud Run environment."
    )

# ── Buckets ────────────────────────────────────────────────────────────────────
# FIX: Read from env so this works across dev / staging / prod without
# changing code. Falls back to the original hardcoded value if not set.
RAW_BUCKET = os.getenv("RAW_BUCKET", "crypto-market-raw-data-us")

# ── BigQuery Datasets ──────────────────────────────────────────────────────────
RAW_DATASET      = "raw_market_data"
STAGING_DATASET  = "staging_market_data"
CURATED_DATASET  = "curated_market_data"
METADATA_DATASET = "platform_metadata"

# ── Tables ─────────────────────────────────────────────────────────────────────
RAW_TABLE      = "crypto_api_responses"
STAGING_TABLE  = "crypto_price_timeseries"
CURATED_TABLE  = "crypto_daily_metrics"
METADATA_TABLE = "pipeline_run_metadata"

# ── Coins (Portfolio Scope) ────────────────────────────────────────────────────
COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "ripple",
    "cardano",
]

# ── Pipeline Modes ─────────────────────────────────────────────────────────────
BACKFILL_DAYS    = int(os.getenv("BACKFILL_DAYS", 90))
INCREMENTAL_DAYS = int(os.getenv("INCREMENTAL_DAYS", 2))   # sliding window

# ── API Settings ───────────────────────────────────────────────────────────────
# Optional: CoinGecko Pro API key — leave blank to use the free tier.
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")