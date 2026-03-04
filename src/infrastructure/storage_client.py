# src/infrastructure/storage_client.py

from google.cloud import storage
from configs.settings import RAW_BUCKET

class StorageClient:

    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(RAW_BUCKET)

    def upload_string(self, destination_blob_name: str, data: str):
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(data)
        return blob.public_url