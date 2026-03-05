from datetime import date, timedelta

from src.ingestion.coingecko_client import CoinGeckoClient
from src.infrastructure.storage_client import StorageClient
from src.infrastructure.bigquery_client import BigQueryClient
from src.metadata.metadata_manager import MetadataManager
from src.utils.logger import get_logger
from configs.settings import PROJECT_ID, BUCKET_NAME

logger = get_logger(__name__)


class IncrementalPipeline:

    def __init__(self):
        self.metadata = MetadataManager(PROJECT_ID)
        self.coingecko = CoinGeckoClient()
        self.gcs = StorageClient(BUCKET_NAME)
        self.bq = BigQueryClient(PROJECT_ID)

    def run(self):

        logger.info("Starting Incremental Pipeline")

        last_date = self.metadata.get_last_processed_date("crypto_pipeline")

        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = date.today() - timedelta(days=1)

        end_date = date.today()

        logger.info("Fetching data from %s to %s", start_date, end_date)

        current_date = start_date

        while current_date <= end_date:

            try:

                logger.info("Processing date %s", current_date)

                data = self.coingecko.fetch_market_data(current_date)

                file_path = f"raw/crypto/{current_date}.json"

                self.gcs.upload_json(data, file_path)

                logger.info("Uploaded data to GCS for %s", current_date)

                current_date += timedelta(days=1)

            except Exception as e:

                logger.error("Failed processing date %s : %s", current_date, str(e))
                raise

        self.metadata.update_last_processed_date(
            "crypto_pipeline",
            end_date
        )

        logger.info("Incremental pipeline completed successfully")