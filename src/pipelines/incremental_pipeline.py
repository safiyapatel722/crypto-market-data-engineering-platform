import json
import pathlib
import time
from datetime import date, timedelta, datetime, timezone

from google.cloud import bigquery

from src.ingestion.coingecko_client import CoinGeckoClient
from src.infrastructure.storage_client import StorageClient
from src.infrastructure.bigquery_client import BigQueryClient
from src.metadata.metadata_manager import MetadataManager
from src.utils.logger import get_logger
from configs.settings import (
    PROJECT_ID,
    RAW_BUCKET,
    RAW_DATASET,
    RAW_TABLE,
    COINS,
    INCREMENTAL_DAYS,
)

logger = get_logger(__name__)

# Path to SQL files — resolved relative to THIS file, not the working directory.
# Works on both Windows and Linux / GCP environments.
_TRANSFORM_DIR = pathlib.Path(__file__).parent.parent / "transformation"
_STAGING_MERGE_SQL  = _TRANSFORM_DIR / "staging_merge.sql"
_CURATED_MERGE_SQL  = _TRANSFORM_DIR / "curated_daily_metrics.sql"

# Seconds to sleep between CoinGecko calls — keeps us well under the
# free-tier limit of ~10–30 req/min.
_API_CALL_DELAY = 1.5

PIPELINE_NAME = "crypto_incremental_pipeline"


class IncrementalPipeline:
    """
    Incremental pipeline that:
      1. Reads last processed date from metadata.
      2. Fetches market chart data from CoinGecko for every coin × every missing date.
      3. Uploads raw JSON to GCS.
      4. Loads raw GCS files into the BigQuery raw table.
      5. Runs staging_merge.sql  →  deduplicates & flattens timeseries.
      6. Runs curated_daily_metrics.sql  →  computes daily aggregates.
      7. Updates metadata only for dates that fully succeeded.
    """

    def __init__(self):
        # FIX: All constructors called with correct (zero) arguments —
        # each class reads its own config from settings.py internally.
        self.metadata   = MetadataManager(PROJECT_ID)
        self.coingecko  = CoinGeckoClient()
        self.gcs        = StorageClient()
        self.bq         = BigQueryClient()

    # ------------------------------------------------------------------ #
    #  Public entry point                                                  #
    # ------------------------------------------------------------------ #

    def run(self) -> bool:
        """
        Run the incremental pipeline.
        Returns True if all dates succeeded, False if any partial failures.
        """
        logger.info("=" * 60)
        logger.info("Starting Incremental Pipeline  [%s]",
                    datetime.now(timezone.utc).isoformat())
        logger.info("=" * 60)

        # ── Step 1: Determine date window ──────────────────────────────
        start_date, end_date = self._resolve_date_window()

        if start_date > end_date:
            logger.info("Pipeline is already up to date. Nothing to process.")
            return True

        logger.info("Processing window: %s → %s", start_date, end_date)

        # ── Step 2: Fetch + upload raw data per coin per date ──────────
        failed_keys   = []   # list of (coin, date) tuples that failed
        uploaded_uris = []   # gs:// URIs successfully written to GCS

        current_date = start_date
        while current_date <= end_date:
            for coin in COINS:
                success, uri = self._process_coin_date(coin, current_date)
                if success:
                    uploaded_uris.append(uri)
                else:
                    failed_keys.append((coin, current_date))
            current_date += timedelta(days=1)

        # ── Step 3: Load all successful GCS files into BigQuery raw ────
        if uploaded_uris:
            self._load_to_bigquery_raw(uploaded_uris)
        else:
            logger.warning("No files were uploaded — skipping BigQuery load.")

        # ── Step 4: Run SQL transformations ────────────────────────────
        self._run_transformations()

        # ── Step 5: Update metadata to the last fully successful date ──
        self._update_metadata(start_date, end_date, failed_keys)

        # ── Step 6: Final report ───────────────────────────────────────
        if failed_keys:
            logger.warning(
                "Pipeline finished with %d failure(s): %s",
                len(failed_keys),
                [(c, str(d)) for c, d in failed_keys],
            )
            return False

        logger.info("Incremental pipeline completed successfully.")
        return True

    # ------------------------------------------------------------------ #
    #  Private helpers                                                     #
    # ------------------------------------------------------------------ #

    def _resolve_date_window(self) -> tuple[date, date]:
        """
        Work out start_date and end_date based on the last run recorded in
        metadata.  Falls back to yesterday if no metadata exists yet.
        """
        last_date = self.metadata.get_last_processed_date(PIPELINE_NAME)

        if last_date:
            # Resume from the day after the last successful run
            start_date = last_date + timedelta(days=1)
        else:
            # First ever run — pick up the last INCREMENTAL_DAYS of data
            start_date = date.today() - timedelta(days=INCREMENTAL_DAYS)

        end_date = date.today() - timedelta(days=1)  # never fetch today (incomplete day)
        return start_date, end_date

    def _process_coin_date(self, coin: str, run_date: date) -> tuple[bool, str | None]:
        """
        Fetch one coin for one date, upload to GCS, and return (success, gs_uri).
        Failures are logged but NOT re-raised so the outer loop continues.
        """
        try:
            logger.info("Fetching  coin=%-10s  date=%s", coin, run_date)

            # FIX: Use the correct method name that actually exists on CoinGeckoClient
            payload = self.coingecko.fetch_market_chart(
                coin_id=coin,
                days=INCREMENTAL_DAYS,
            )

            # Validate that CoinGecko returned the expected structure
            self._validate_payload(payload, coin)

            # Build the raw record — same schema as backfill_pipeline
            ingestion_time = datetime.now(timezone.utc).isoformat()
            record = {
                "ingestion_time": ingestion_time,
                "coin_id":        coin,
                "api_days_requested": INCREMENTAL_DAYS,
                # FIX: store payload as a JSON string (consistent with backfill
                # and what staging_merge.sql's JSON_QUERY_ARRAY expects)
                "payload": json.dumps(payload),
            }

            # GCS path:  raw/<coin>/<date>/<ingestion_timestamp>.json
            # Including date in the path makes it easy to re-process a single day.
            blob_name = f"raw/{coin}/{run_date}/{ingestion_time}.json"

            # FIX: use upload_string (the method that actually exists)
            gcs_uri = self.gcs.upload_string(
                destination_blob_name=blob_name,
                data=json.dumps(record),
            )

            # StorageClient.upload_string currently returns blob.public_url which
            # is None for private buckets — build the gs:// URI ourselves.
            gs_uri = f"gs://{RAW_BUCKET}/{blob_name}"
            logger.info("Uploaded  coin=%-10s  date=%s  →  %s", coin, run_date, gs_uri)

            # Respect CoinGecko rate limits between calls
            time.sleep(_API_CALL_DELAY)

            return True, gs_uri

        except Exception as exc:
            # FIX: log and continue instead of re-raising so one failure
            # doesn't kill the entire pipeline run
            logger.error(
                "FAILED    coin=%-10s  date=%s  error=%s",
                coin, run_date, exc,
            )
            return False, None

    def _validate_payload(self, payload: dict, coin: str) -> None:
        """
        Guard against CoinGecko returning a 200 with empty or missing arrays
        (can happen for low-volume or delisted coins).
        """
        required = ["prices", "market_caps", "total_volumes"]
        missing  = [k for k in required if not payload.get(k)]
        if missing:
            raise ValueError(
                f"CoinGecko response for '{coin}' is missing or empty: {missing}"
            )

    def _load_to_bigquery_raw(self, uris: list[str]) -> None:
        """
        Load all successfully uploaded GCS files into the BigQuery raw table
        in one batch load job.
        """
        table_id = f"{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            # Ignore unknown fields so schema additions don't break the load
            ignore_unknown_values=True,
        )

        logger.info("Loading %d file(s) into BigQuery raw table: %s",
                    len(uris), table_id)

        # FIX: load_json_to_table accepts a list of URIs — batch in one job
        # instead of one job per file (cheaper + faster)
        self.bq.load_json_to_table(
            uri=uris,          # BigQuery accepts a list here
            table_id=table_id,
            job_config=job_config,
        )

        logger.info("BigQuery raw load complete.")

    def _run_transformations(self) -> None:
        """
        Run staging merge then curated metrics SQL, in order.
        Uses __file__-relative paths so this works regardless of where
        the script is launched from.
        """
        steps = [
            ("Staging merge",         _STAGING_MERGE_SQL),
            ("Curated daily metrics", _CURATED_MERGE_SQL),
        ]

        for label, sql_path in steps:
            logger.info("Running transformation: %s", label)
            try:
                query = sql_path.read_text(encoding="utf-8")
                self.bq.execute_query(query)
                logger.info("Completed: %s", label)
            except Exception as exc:
                # Transformation failures are logged but don't prevent
                # metadata update — raw data is safely in BQ already.
                logger.error("Transformation FAILED [%s]: %s", label, exc)
                raise   # re-raise so caller knows transforms didn't finish

    def _update_metadata(
        self,
        start_date: date,
        end_date: date,
        failed_keys: list[tuple[str, date]],
    ) -> None:
        """
        Update the metadata pointer to reflect the last date where ALL
        coins succeeded.  Avoids advancing past a partially failed date.
        """
        if not failed_keys:
            # Everything succeeded — advance to end_date
            self.metadata.update_last_processed_date(PIPELINE_NAME, end_date)
            logger.info("Metadata updated to %s", end_date)
            return

        # FIX: find the last date where every coin succeeded
        failed_dates = {d for _, d in failed_keys}
        all_dates    = [
            start_date + timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]
        successful_dates = [d for d in all_dates if d not in failed_dates]

        if successful_dates:
            last_good = max(successful_dates)
            self.metadata.update_last_processed_date(PIPELINE_NAME, last_good)
            logger.info(
                "Partial success — metadata updated to %s  "
                "(failed dates will be retried next run: %s)",
                last_good,
                sorted(failed_dates),
            )
        else:
            # Every date failed — do NOT advance the metadata pointer at all
            logger.error(
                "All dates failed. Metadata pointer NOT updated. "
                "Next run will retry from %s.",
                start_date,
            )




















