# tests/test_storage_client.py

import pytest
from unittest.mock import patch, MagicMock
from google.cloud.exceptions import GoogleCloudError

from src.infrastructure.storage_client import StorageClient


# ── FIXTURE ───────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_gcs():
    with patch("src.infrastructure.storage_client.storage.Client") as mock_gcs_class:
        mock_instance = mock_gcs_class.return_value
        mock_bucket   = mock_instance.bucket.return_value
        mock_blob     = mock_bucket.blob.return_value

        yield {
            "gcs_class": mock_gcs_class,
            "instance":  mock_instance,
            "bucket":    mock_bucket,
            "blob":      mock_blob,
        }


# ── TEST 1 — GCS client created on init ───────────────────────────────────────

def test_storage_client_creates_gcs_client(mock_gcs):
    StorageClient("my-bucket")
    mock_gcs["gcs_class"].assert_called_once()


# ── TEST 2 — Bucket created with correct name ─────────────────────────────────

def test_storage_client_creates_bucket_with_correct_name(mock_gcs):
    StorageClient("my-bucket")
    mock_gcs["instance"].bucket.assert_called_once_with("my-bucket")


# ── TEST 3 — upload_string sets content_type application/json ─────────────────

def test_upload_string_sets_correct_content_type(mock_gcs):
    client = StorageClient("my-bucket")
    client.upload_string("raw/bitcoin.json", '{"coin": "bitcoin"}')

    # check upload_from_string was called with correct content_type
    call_kwargs = mock_gcs["blob"].upload_from_string.call_args
    assert call_kwargs.kwargs["content_type"] == "application/json"


# ── TEST 4 — upload_string returns gs:// URI ──────────────────────────────────

def test_upload_string_returns_gs_uri(mock_gcs):
    # bucket.name is used to build the gs:// URI
    mock_gcs["bucket"].name = "my-bucket"

    client = StorageClient("my-bucket")
    result = client.upload_string("raw/bitcoin.json", '{"coin": "bitcoin"}')

    assert result.startswith("gs://")
    assert "my-bucket" in result
    assert "raw/bitcoin.json" in result


# ── TEST 5 — upload_string raises when GCS fails ──────────────────────────────

def test_upload_string_raises_when_gcs_fails(mock_gcs):
    mock_gcs["blob"].upload_from_string.side_effect = Exception("GCS is down")

    client = StorageClient("my-bucket")

    with pytest.raises(Exception, match="GCS is down"):
        client.upload_string("raw/bitcoin.json", '{"coin": "bitcoin"}')


# ── TEST 6 — blob_exists returns True when blob exists ────────────────────────

def test_blob_exists_returns_true_when_exists(mock_gcs):
    mock_gcs["bucket"].blob.return_value.exists.return_value = True

    client = StorageClient("my-bucket")
    result = client.blob_exists("raw/bitcoin.json")

    assert result is True


# ── TEST 7 — blob_exists returns False when blob does not exist ───────────────

def test_blob_exists_returns_false_when_not_exists(mock_gcs):
    mock_gcs["bucket"].blob.return_value.exists.return_value = False

    client = StorageClient("my-bucket")
    result = client.blob_exists("raw/bitcoin.json")

    assert result is False