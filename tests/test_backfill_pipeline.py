# tests/test_backfill_pipeline.py

import pytest
from unittest.mock import patch, MagicMock

from src.pipelines.backfill_pipeline import BackfillPipeline


# ── FIXTURE ───────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_pipeline():
    """
    Patches all three external dependencies of BackfillPipeline:
      - CoinGeckoClient
      - StorageClient
      - BigQueryClient

    Each is replaced with a MagicMock so no real API or GCP calls happen.
    """
    with patch("src.pipelines.backfill_pipeline.CoinGeckoClient") as mock_cg, \
         patch("src.pipelines.backfill_pipeline.StorageClient")    as mock_sc, \
         patch("src.pipelines.backfill_pipeline.BigQueryClient")   as mock_bq:

        # instances returned when each class is constructed
        mock_cg_instance = mock_cg.return_value
        mock_sc_instance = mock_sc.return_value
        mock_bq_instance = mock_bq.return_value

        # default happy-path return values
        mock_cg_instance.fetch_market_chart.return_value = {
            "prices":        [[1704067200000, 42000.0]],
            "market_caps":   [[1704067200000, 820000000000.0]],
            "total_volumes": [[1704067200000, 15000000000.0]],
        }
        mock_sc_instance.upload_string.return_value = "gs://bucket/raw/bitcoin/file.json"
        mock_sc_instance.blob_exists.return_value   = False  # not already loaded

        mock_load_job        = MagicMock()
        mock_load_job.errors = None
        mock_bq_instance.load_json_to_table.return_value = mock_load_job

        mock_query_job        = MagicMock()
        mock_query_job.errors = None
        mock_bq_instance.execute_query.return_value = mock_query_job

        yield {
            "pipeline":    BackfillPipeline(),
            "coingecko":   mock_cg_instance,
            "storage":     mock_sc_instance,
            "bq":          mock_bq_instance,
        }


# ── TEST 1 — fetches data for all 5 coins ────────────────────────────────────

def test_run_fetches_data_for_all_coins(mock_pipeline):
    mock_pipeline["pipeline"].run()

    assert mock_pipeline["coingecko"].fetch_market_chart.call_count == 5


# ── TEST 2 — uploads one file per coin ───────────────────────────────────────

def test_run_uploads_one_file_per_coin(mock_pipeline):
    mock_pipeline["pipeline"].run()

    assert mock_pipeline["storage"].upload_string.call_count == 5


# ── TEST 3 — loads all URIs in one batch job not 5 separate jobs ─────────────

def test_run_loads_all_uris_in_one_batch_job(mock_pipeline):
    mock_pipeline["pipeline"].run()

    # load_json_to_table should be called exactly ONCE with all URIs
    assert mock_pipeline["bq"].load_json_to_table.call_count == 1

    call_args = mock_pipeline["bq"].load_json_to_table.call_args
    uris      = call_args.kwargs.get("uri") or call_args.args[0]

    assert len(uris) == 5


# ── TEST 4 — runs staging SQL after loading ───────────────────────────────────

def test_run_executes_staging_sql(mock_pipeline):
    with patch("src.pipelines.backfill_pipeline._STAGING_MERGE_SQL") as mock_sql:
        mock_sql.read_text.return_value = "MERGE staging..."
        mock_pipeline["pipeline"].run()

    # execute_query should be called at least twice (staging + curated)
    assert mock_pipeline["bq"].execute_query.call_count >= 2


# ── TEST 5 — skips coin if already loaded in GCS ─────────────────────────────

def test_run_skips_coin_already_loaded(mock_pipeline):
    # make blob_exists return True for all coins — all already loaded
    mock_pipeline["storage"].blob_exists.return_value = True

    # idempotency check hits BQ not GCS in this implementation
    # so we mock the BQ already_loaded check to return True
    with patch.object(
        mock_pipeline["pipeline"], "_already_loaded", return_value=True
    ):
        mock_pipeline["pipeline"].run()

    # nothing should be fetched or uploaded
    mock_pipeline["coingecko"].fetch_market_chart.assert_not_called()
    mock_pipeline["storage"].upload_string.assert_not_called()


# ── TEST 6 — continues to next coin if one coin fails ────────────────────────

def test_run_continues_when_one_coin_fails(mock_pipeline):
    # make fetch fail for the first call only, succeed for the rest
    mock_pipeline["coingecko"].fetch_market_chart.side_effect = [
        Exception("API error"),   # bitcoin fails
        mock_pipeline["coingecko"].fetch_market_chart.return_value,  # ethereum
        mock_pipeline["coingecko"].fetch_market_chart.return_value,  # solana
        mock_pipeline["coingecko"].fetch_market_chart.return_value,  # ripple
        mock_pipeline["coingecko"].fetch_market_chart.return_value,  # cardano
    ]

    result = mock_pipeline["pipeline"].run()

    # pipeline should not crash — returns False for partial failure
    assert result is False
    # other 4 coins should still be fetched
    assert mock_pipeline["coingecko"].fetch_market_chart.call_count == 5


# ── TEST 7 — still runs transforms even when one coin failed ─────────────────

def test_run_executes_transforms_even_on_partial_failure(mock_pipeline):
    mock_pipeline["coingecko"].fetch_market_chart.side_effect = [
        Exception("API error"),
        mock_pipeline["coingecko"].fetch_market_chart.return_value,
        mock_pipeline["coingecko"].fetch_market_chart.return_value,
        mock_pipeline["coingecko"].fetch_market_chart.return_value,
        mock_pipeline["coingecko"].fetch_market_chart.return_value,
    ]

    mock_pipeline["pipeline"].run()

    # transforms must still run — partial data is better than no data
    assert mock_pipeline["bq"].execute_query.call_count >= 2
