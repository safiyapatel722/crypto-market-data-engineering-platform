from google.cloud import storage
import json

from src.utils.logger import get_logger

logger = get_logger(__name__)


class StorageClient:

    def __init__(self, bucket_name: str):

        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_json(self, destination_blob_name: str, data: dict):

        try:

            logger.info("Uploading file to GCS: %s", destination_blob_name)

            blob = self.bucket.blob(destination_blob_name)

            blob.upload_from_string(
                json.dumps(data),
                content_type="application/json"
            )

            logger.info("Upload successful: %s", destination_blob_name)

            return blob.public_url

        except Exception as e:

            logger.error(
                "Failed to upload file %s : %s",
                destination_blob_name,
                str(e)
            )

            raise