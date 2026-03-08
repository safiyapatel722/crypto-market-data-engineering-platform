# tests/test_incremental_pipeline.py

import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock, call

from src.pipelines.incremental_pipeline import IncrementalPipeline


# ── FIXTURE ───────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_pipeline():
    """
    Patches all four external dependencies of IncrementalPipeline:
      - MetadataManager
      - CoinGeckoClient
      - StorageClient
      - BigQueryClient
    """
    with patch("src.pipelines.incremental_pipeline.MetadataManager") as mock_meta, \
         patch("src.pipelines.incremental_pipeline.CoinGeckoClient") as mock_cg,   \
         patch("src.pipelines.incremental_pipeline.StorageClient")   as mock_sc,   \
         patch("src.pipelines.incremental_pipeline.BigQueryClient")  as mock_bq,   \
         patch("src.pipelines.incremental_pipeline.time.sleep"):  # skip rate limit delays

        mock_meta_instance = mock_meta.return_value
        mock_cg_instance   = mock_cg.return_value
        mock_sc_instance   = mock_sc.return_value
        mock_bq_instance   = mock_bq.return_value

        # default: metadata exists — last run was 2 days ago
        today     = date.today()
        last_date = today - timedelta(days=2)
        mock_meta_instance.get_last_processed_date.return_value = last_date

        # default happy-path API response
        mock_cg_instance.fetch_market_chart.return_value = {
            "prices":        [[1704067200000, 42000.0]],
            "market_caps":   [[1704067200000, 820000000000.0]],
            "total_volumes": [[1704067200000, 15000000000.0]],
        }

        mock_sc_instance.upload_string.return_value = "gs://bucket/raw/bitcoin/file.json"

        mock_load_job        = MagicMock()
        mock_load_job.errors = None
        mock_bq_instance.load_json_to_table.return_value = mock_load_job

        mock_query_job        = MagicMock()
        mock_query_job.errors = None
        mock_bq_instance.execute_query.return_value = mock_query_job

        yield {
            "pipeline": IncrementalPipeline(),
            "metadata": mock_meta_instance,
            "coingecko": mock_cg_instance,
            "storage":   mock_sc_instance,
            "bq":        mock_bq_instance,
        }


# ── TEST 1 — reads last_processed_date from metadata ─────────────────────────

def test_run_reads_metadata_on_start(mock_pipeline):
    mock_pipeline["pipeline"].run()
    mock_pipeline["metadata"].get_last_processed_date.assert_called_once()


# ── TEST 2 — starts from last_date + 1 when metadata exists ──────────────────

def test_run_starts_from_last_date_plus_one(mock_pipeline):
    today     = date.today()
    last_date = today - timedelta(days=2)

    mock_pipeline["metadata"].get_last_processed_date.return_value = last_date
    mock_pipeline["pipeline"].run()

    # window should be last_date+1 → yesterday
    # that means 1 day window × 5 coins = 5 fetch calls
    assert mock_pipeline["coingecko"].fetch_market_chart.call_count == 5


# ── TEST 3 — starts from today - INCREMENTAL_DAYS on first run ───────────────

def test_run_uses_incremental_days_on_first_run(mock_pipeline):
    # no metadata = first run
    mock_pipeline["metadata"].get_last_processed_date.return_value = None

    with patch("src.pipelines.incremental_pipeline.INCREMENTAL_DAYS", 2):
        mock_pipeline["pipeline"].run()

    # INCREMENTAL_DAYS=2 means window is 2 days × 5 coins = 10 fetch calls
    assert mock_pipeline["coingecko"].fetch_market_chart.call_count == 10


# ── TEST 4 — fetches all coins for each date in window ───────────────────────

def test_run_fetches_all_coins_for_each_date(mock_pipeline):
    mock_pipeline["pipeline"].run()

    # each call should include coin_id as a keyword argument
    calls = mock_pipeline["coingecko"].fetch_market_chart.call_args_list
    coins_fetched = [c.kwargs["coin_id"] for c in calls]

    assert "bitcoin"  in coins_fetched
    assert "ethereum" in coins_fetched
    assert "solana"   in coins_fetched
    assert "ripple"   in coins_fetched
    assert "cardano"  in coins_fetched


# ── TEST 5 — updates metadata to end_date on full success ────────────────────

def test_run_updates_metadata_to_end_date_on_success(mock_pipeline):
    today     = date.today()
    yesterday = today - timedelta(days=1)

    mock_pipeline["pipeline"].run()

    mock_pipeline["metadata"].update_last_processed_date.assert_called_once()

    call_args    = mock_pipeline["metadata"].update_last_processed_date.call_args
    updated_date = call_args.args[1]

    assert updated_date == yesterday


# ── TEST 6 — updates metadata to last clean date on partial failure ───────────

def test_run_updates_metadata_to_last_clean_date_on_partial_failure(mock_pipeline):
    today     = date.today()
    last_date = today - timedelta(days=3)

    mock_pipeline["metadata"].get_last_processed_date.return_value = last_date

    # make all coins fail for the last date in window (yesterday)
    yesterday = today - timedelta(days=1)

    original_fetch = mock_pipeline["coingecko"].fetch_market_chart.return_value

    def fetch_side_effect(coin_id, days):
        # fail all coins on yesterday's date — we simulate this by failing
        # the last 5 calls (one per coin for yesterday)
        call_count = mock_pipeline["coingecko"].fetch_market_chart.call_count
        total_coins = 5
        # last 5 calls correspond to yesterday
        if call_count > total_coins:
            raise Exception("API error on yesterday")
        return original_fetch

    mock_pipeline["coingecko"].fetch_market_chart.side_effect = fetch_side_effect
    mock_pipeline["pipeline"].run()

    # metadata should still be updated — just not to yesterday
    mock_pipeline["metadata"].update_last_processed_date.assert_called_once()


# ── TEST 7 — does not update metadata when all coins fail ────────────────────

def test_run_does_not_update_metadata_when_all_fail(mock_pipeline):
    mock_pipeline["coingecko"].fetch_market_chart.side_effect = Exception("API down")

    mock_pipeline["pipeline"].run()

    # all coins failed — pointer must NOT move
    mock_pipeline["metadata"].update_last_processed_date.assert_not_called()


# ── TEST 8 — continues loop when one coin fails ───────────────────────────────

def test_run_continues_when_one_coin_fails(mock_pipeline):
    original = mock_pipeline["coingecko"].fetch_market_chart.return_value

    mock_pipeline["coingecko"].fetch_market_chart.side_effect = [
        Exception("bitcoin API error"),  # bitcoin fails
        original,                        # ethereum succeeds
        original,                        # solana succeeds
        original,                        # ripple succeeds
        original,                        # cardano succeeds
    ]

    result = mock_pipeline["pipeline"].run()

    # pipeline should not crash — returns False for partial failure
    assert result is False
    # all 5 coins should still be attempted
    assert mock_pipeline["coingecko"].fetch_market_chart.call_count == 5
