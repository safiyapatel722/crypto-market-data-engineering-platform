# src/infrastructure/storage_client.py

from google.api_core.retry import Retry
from google.cloud import storage

from configs.settings import RAW_BUCKET
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StorageClient:
    """
    Thin wrapper around the Google Cloud Storage SDK.

    Fixes applied vs original:
      - Returns gs:// URI instead of blob.public_url
        (public_url is None for private buckets — which yours should be)
      - Sets content_type="application/json" on every upload
      - Adds a GCP-native Retry policy for transient network errors
      - Logs every upload with the destination URI
      - bucket_name is injectable via constructor — reusable for multiple buckets
      - Added blob_exists() helper for idempotency checks
    """

    def __init__(self, bucket_name: str = RAW_BUCKET):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        logger.info("StorageClient initialised — bucket: %s", bucket_name)

    # ── Public methods ─────────────────────────────────────────────────────────

    def upload_string(self, destination_blob_name: str, data: str) -> str:
        """
        Upload a string (typically a JSON document) to GCS.

        Args:
            destination_blob_name: GCS object path,
                                   e.g. "raw/bitcoin/2025-01-01/data.json"
            data:                  String content to upload.

        Returns:
            The gs:// URI of the uploaded object,
            e.g. "gs://crypto-market-raw-data-us/raw/bitcoin/2025-01-01/data.json"

        Raises:
            google.cloud.exceptions.GoogleCloudError: On upload failure after retries.
        """
        blob = self.bucket.blob(destination_blob_name)

        blob.upload_from_string(
            data,
            # FIX: Set content_type so GCS identifies the file correctly.
            # Without it, files default to application/octet-stream which
            # breaks preview in the GCS console and some downstream tools.
            content_type = "application/json",

            # FIX: GCP retry policy retries transient network errors
            # automatically for up to 60 seconds. Without this, a brief
            # GCS hiccup causes the pipeline to crash and leaves a missing
            # raw file that breaks the downstream BQ load job.
            retry = Retry(deadline=60),
        )

        # FIX: Return gs:// URI, not blob.public_url.
        # blob.public_url is None for private buckets (which yours is).
        # Pipelines and BigQuery load jobs use gs:// URIs anyway.
        gs_uri = f"gs://{self.bucket.name}/{destination_blob_name}"
        logger.info("Uploaded → %s", gs_uri)
        return gs_uri

    def blob_exists(self, blob_name: str) -> bool:
        """
        Check whether a GCS object already exists.
        Use this for idempotency checks before uploading to avoid
        re-uploading data on pipeline reruns.

        Args:
            blob_name: GCS object path to check.

        Returns:
            True if the object exists, False otherwise.
        """
        exists = self.bucket.blob(blob_name).exists()
        logger.info("Blob exists? %s — %s", exists, blob_name)
        return exists