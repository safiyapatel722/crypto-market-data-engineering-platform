# tests/test_bigquery_client.py

import pytest
from unittest.mock import patch, MagicMock
from google.api_core.exceptions import GoogleAPIError

from src.infrastructure.bigquery_client import BigQueryClient


# ── FIXTURE ───────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_bq():
    with patch("src.infrastructure.bigquery_client.bigquery.Client") as mock_bq_class:
        mock_instance = mock_bq_class.return_value

        yield {
            "bq_class":  mock_bq_class,
            "instance":  mock_instance,
        }


# ── TEST 1 — BQ client created on init ───────────────────────────────────────

def test_bigquery_client_creates_client(mock_bq):
    BigQueryClient()
    mock_bq["bq_class"].assert_called_once()


# ── TEST 2 — execute_query runs and returns job ───────────────────────────────

def test_execute_query_returns_job(mock_bq):
    mock_job          = MagicMock()
    mock_job.errors   = None
    mock_bq["instance"].query.return_value = mock_job

    client = BigQueryClient()
    result = client.execute_query("SELECT 1")

    mock_bq["instance"].query.assert_called_once_with("SELECT 1")
    assert result == mock_job


# ── TEST 3 — execute_query raises RuntimeError when job.errors not empty ──────

def test_execute_query_raises_when_job_has_errors(mock_bq):
    mock_job        = MagicMock()
    mock_job.errors = [{"reason": "invalid", "message": "bad query"}]
    mock_bq["instance"].query.return_value = mock_job

    client = BigQueryClient()

    with pytest.raises(RuntimeError, match="errors"):
        client.execute_query("SELECT bad stuff")


# ── TEST 4 — execute_query raises GoogleAPIError on BQ failure ────────────────

def test_execute_query_raises_on_google_api_error(mock_bq):
    mock_bq["instance"].query.side_effect = GoogleAPIError("BQ unavailable")

    client = BigQueryClient()

    with pytest.raises(GoogleAPIError):
        client.execute_query("SELECT 1")


# ── TEST 5 — load_json_to_table loads URIs into correct table ─────────────────

def test_load_json_to_table_loads_correct_table(mock_bq):
    mock_load_job        = MagicMock()
    mock_load_job.errors = None
    mock_bq["instance"].load_table_from_uri.return_value = mock_load_job

    client     = BigQueryClient()
    job_config = MagicMock()

    client.load_json_to_table(
        uri       = ["gs://bucket/file1.json", "gs://bucket/file2.json"],
        table_id  = "project.dataset.table",
        job_config= job_config,
    )

    mock_bq["instance"].load_table_from_uri.assert_called_once_with(
        ["gs://bucket/file1.json", "gs://bucket/file2.json"],
        "project.dataset.table",
        job_config=job_config,
    )


# ── TEST 6 — load_json_to_table accepts single string URI ────────────────────

def test_load_json_to_table_accepts_single_string_uri(mock_bq):
    mock_load_job        = MagicMock()
    mock_load_job.errors = None
    mock_bq["instance"].load_table_from_uri.return_value = mock_load_job

    client = BigQueryClient()

    # pass a single string — should be normalised to a list internally
    client.load_json_to_table(
        uri        = "gs://bucket/file1.json",
        table_id   = "project.dataset.table",
        job_config = MagicMock(),
    )

    call_args = mock_bq["instance"].load_table_from_uri.call_args
    assert call_args.args[0] == ["gs://bucket/file1.json"]


# ── TEST 7 — load_json_to_table raises RuntimeError when job.errors not empty ─

def test_load_json_to_table_raises_when_job_has_errors(mock_bq):
    mock_load_job        = MagicMock()
    mock_load_job.errors = [{"reason": "notFound", "message": "table not found"}]
    mock_bq["instance"].load_table_from_uri.return_value = mock_load_job

    client = BigQueryClient()

    with pytest.raises(RuntimeError, match="errors"):
        client.load_json_to_table(
            uri        = ["gs://bucket/file.json"],
            table_id   = "project.dataset.table",
            job_config = MagicMock(),
        )
