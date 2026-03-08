# tests/test_metadata_manager.py

import pytest
from datetime import date
from unittest.mock import patch, MagicMock
from google.api_core.exceptions import GoogleAPIError

from src.metadata.metadata_manager import MetadataManager


# ── FIXTURE ───────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_bq():
    with patch("src.metadata.metadata_manager.bigquery.Client") as mock_bq_class:
        mock_instance = mock_bq_class.return_value

        yield {
            "bq_class": mock_bq_class,
            "instance": mock_instance,
        }


# ── TEST 1 — returns date when record exists ──────────────────────────────────

def test_get_last_processed_date_returns_date_when_exists(mock_bq):
    # ARRANGE — fake a BQ result row with a date
    mock_row                    = MagicMock()
    mock_row.last_processed_date = date(2025, 1, 14)

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([mock_row]))

    mock_job = MagicMock()
    mock_job.result.return_value = mock_result
    mock_bq["instance"].query.return_value = mock_job

    # ACT
    manager = MetadataManager("my-project")
    result  = manager.get_last_processed_date("crypto_incremental_pipeline")

    # ASSERT
    assert result == date(2025, 1, 14)


# ── TEST 2 — returns None on first run (no rows) ──────────────────────────────

def test_get_last_processed_date_returns_none_on_first_run(mock_bq):
    # ARRANGE — fake an empty BQ result
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))  # no rows

    mock_job = MagicMock()
    mock_job.result.return_value = mock_result
    mock_bq["instance"].query.return_value = mock_job

    # ACT
    manager = MetadataManager("my-project")
    result  = manager.get_last_processed_date("crypto_incremental_pipeline")

    # ASSERT
    assert result is None


# ── TEST 3 — query uses @pipeline_name parameter not f-string ─────────────────

def test_get_last_processed_date_uses_parameterized_query(mock_bq):
    # ARRANGE
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))

    mock_job = MagicMock()
    mock_job.result.return_value = mock_result
    mock_bq["instance"].query.return_value = mock_job

    # ACT
    manager = MetadataManager("my-project")
    manager.get_last_processed_date("crypto_incremental_pipeline")

    # ASSERT — the SQL should use @pipeline_name not a hardcoded string
    call_args = mock_bq["instance"].query.call_args
    sql       = call_args.args[0]

    assert "@pipeline_name" in sql
    assert "crypto_incremental_pipeline" not in sql   # value not injected directly


# ── TEST 4 — update calls query with correct pipeline name and date ────────────

def test_update_last_processed_date_calls_query(mock_bq):
    mock_job = MagicMock()
    mock_bq["instance"].query.return_value = mock_job

    manager = MetadataManager("my-project")
    manager.update_last_processed_date("crypto_incremental_pipeline", date(2025, 1, 16))

    # query should have been called once
    mock_bq["instance"].query.assert_called_once()


# ── TEST 5 — update raises GoogleAPIError on BQ failure ───────────────────────

def test_update_last_processed_date_raises_on_failure(mock_bq):
    mock_bq["instance"].query.side_effect = GoogleAPIError("BQ unavailable")

    manager = MetadataManager("my-project")

    with pytest.raises(GoogleAPIError):
        manager.update_last_processed_date(
            "crypto_incremental_pipeline",
            date(2025, 1, 16),
        )
