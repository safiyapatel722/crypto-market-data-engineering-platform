# src/pipelines/backfill_pipeline.py

import json
from datetime import datetime
from google.cloud import bigquery
from configs.settings import (
    PROJECT_ID,
    RAW_BUCKET,
    RAW_DATASET,
    RAW_TABLE,
    BACKFILL_DAYS,
    COINS
)
from src.ingestion.coingecko_client import CoinGeckoClient
from src.infrastructure.storage_client import StorageClient
from src.infrastructure.bigquery_client import BigQueryClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BackfillPipeline:

    def __init__(self):
        self.api_client = CoinGeckoClient()
        self.storage_client = StorageClient()
        self.bq_client = BigQueryClient()

    def run(self):

        for coin in COINS:
            logger.info(f"Starting backfill for {coin}")

            # 1️⃣ Fetch API Data
            payload = self.api_client.fetch_market_chart(
                coin_id=coin,
                days=BACKFILL_DAYS
            )

            ingestion_time = datetime.utcnow().isoformat()

            record = {
                "ingestion_time": ingestion_time,
                "coin_id": coin,
                "api_days_requested": BACKFILL_DAYS,
                "payload": json.dumps(payload)
            }

            file_name = f"raw/{coin}/{ingestion_time}.json"

            # 2️⃣ Upload to GCS
            self.storage_client.upload_string(
                destination_blob_name=file_name,
                data=json.dumps(record)
            )

            logger.info(f"Uploaded raw file for {coin}")

            # 3️⃣ Load into BigQuery Raw Table
            uri = f"gs://{RAW_BUCKET}/{file_name}"
            table_id = f"{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition="WRITE_APPEND",
            )

            self.bq_client.load_json_to_table(
                uri=uri,
                table_id=table_id,
                job_config=job_config
            )
            logger.info(f"Loaded raw data into BigQuery for {coin}")

        # 4️⃣ Run staging merge
        with open("src\\transformation\staging_merge.sql", "r") as file:
            merge_query = file.read()
            self.bq_client.execute_query(merge_query)
            logger.info("Staging merge completed.")
            
        # 5️⃣ Run curated merge
        with open("src\\transformation\curated_daily_metrics.sql", "r") as file:
             curated_query = file.read()
             self.bq_client.execute_query(curated_query)
             logger.info("Curated daily metrics merge completed.")
            

        logger.info("Backfill completed successfully.")