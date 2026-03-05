# src/pipelines/backfill_pipeline.py

import json
import pathlib
import time
from datetime import datetime, timezone

from google.cloud import bigquery

from configs.settings import (
    PROJECT_ID,
    RAW_BUCKET,
    RAW_DATASET,
    RAW_TABLE,
    BACKFILL_DAYS,
    COINS,
)
from src.ingestion.coingecko_client import CoinGeckoClient
from src.infrastructure.storage_client import StorageClient
from src.infrastructure.bigquery_client import BigQueryClient
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── SQL file paths ─────────────────────────────────────────────────────────────
# Resolved relative to THIS file so it works on both Windows and Linux/GCP.
# No more hardcoded backslashes that break on Cloud Run.
_TRANSFORM_DIR      = pathlib.Path(__file__).parent.parent / "transformation"
_STAGING_MERGE_SQL  = _TRANSFORM_DIR / "staging_merge.sql"
_CURATED_MERGE_SQL  = _TRANSFORM_DIR / "curated_daily_metrics.sql"

# Seconds to wait between CoinGecko API calls.
# Free tier limit is ~10-30 req/min — 1.5s keeps us safely under that.
_API_CALL_DELAY = 1.5

PIPELINE_NAME = "crypto_backfill_pipeline"


class BackfillPipeline:
    """
    Backfill pipeline that loads the last BACKFILL_DAYS of market data
    for every coin in COINS.

    Steps per coin:
      1. Check idempotency — skip if today's data already exists in raw table.
      2. Fetch market chart from CoinGecko API.
      3. Validate the response structure.
      4. Upload raw JSON record to GCS.
      5. Load GCS file into BigQuery raw table.

    After all coins:
      6. Run staging_merge.sql   → flatten & deduplicate timeseries.
      7. Run curated_daily_metrics.sql → compute daily aggregates.

    Failures per coin are isolated — one bad coin does NOT stop the others.
    The SQL transforms run even if some coins failed (partial data is better
    than no data).
    """

    def __init__(self):
        self.api_client     = CoinGeckoClient()
        self.storage_client = StorageClient()
        self.bq_client      = BigQueryClient()

    # ── Public entry point ────────────────────────────────────────────────────

    def run(self) -> bool:
        """
        Execute the full backfill pipeline.
        Returns True if all coins succeeded, False on partial failure.
        """
        logger.info("=" * 60)
        logger.info("Starting Backfill Pipeline  [%s]",
                    datetime.now(timezone.utc).isoformat())
        logger.info("Coins: %s  |  Days: %d", COINS, BACKFILL_DAYS)
        logger.info("=" * 60)

        # ── Phase 1: Fetch → GCS → BigQuery raw, per coin ─────────────
        failed_coins    = []
        uploaded_uris   = []

        for coin in COINS:
            success, uri = self._process_coin(coin)
            if success and uri:
                uploaded_uris.append(uri)
            elif not success:
                failed_coins.append(coin)

        # ── Phase 2: Batch load all successful files into BQ raw ───────
        if uploaded_uris:
            self._load_to_bigquery(uploaded_uris)
        else:
            logger.warning("No files uploaded — skipping BigQuery load.")

        # ── Phase 3: Run SQL transformations ───────────────────────────
        # Always run transforms even on partial failure — whatever data
        # landed in raw should flow through to staging and curated.
        self._run_transformations()

        # ── Phase 4: Summary ───────────────────────────────────────────
        if failed_coins:
            logger.warning(
                "Backfill finished with %d failure(s). "
                "Coins to retry: %s",
                len(failed_coins), failed_coins,
            )
            return False

        logger.info("Backfill pipeline completed successfully.")
        return True

    # ── Private helpers ───────────────────────────────────────────────────────

    def _process_coin(self, coin: str) -> tuple[bool, str | None]:
        """
        Run the full fetch → validate → upload → (return URI) flow for one coin.
        Returns (success, gs_uri).  Never raises — failures are logged and
        returned as (False, None) so the loop keeps going for other coins.
        """
        try:
            logger.info("── Processing coin: %s ──", coin)

            # ── Step 1: Idempotency check ──────────────────────────────
            # Prevents duplicate rows in the raw table when the backfill
            # is re-run on the same day (e.g. after a partial failure).
            today = datetime.now(timezone.utc).date().isoformat()
            if self._already_loaded(coin, today):
                logger.info(
                    "Skipping %s — raw data for %s already exists.", coin, today
                )
                return True, None   # not a failure, just nothing to do

            # ── Step 2: Fetch from CoinGecko ───────────────────────────
            payload = self.api_client.fetch_market_chart(
                coin_id=coin,
                days=BACKFILL_DAYS,
            )

            # ── Step 3: Validate response ──────────────────────────────
            self._validate_payload(payload, coin)

            # ── Step 4: Build raw record ───────────────────────────────
            # ingestion_time is timezone-aware ISO string (unambiguous in BQ)
            ingestion_time = datetime.now(timezone.utc).isoformat()

            record = {
                "ingestion_time":     ingestion_time,
                "coin_id":            coin,
                "api_days_requested": BACKFILL_DAYS,
                # payload stored as JSON string — matches what staging_merge.sql
                # expects when it calls JSON_QUERY_ARRAY(r.payload, "$.prices")
                "payload": json.dumps(payload),
            }

            # GCS path: raw/<coin>/<date>/<timestamp>.json
            # Partitioning by date makes it easy to audit or reprocess one day.
            blob_name = f"raw/{coin}/{today}/{ingestion_time}.json"

            # ── Step 5: Upload to GCS ──────────────────────────────────
            self.storage_client.upload_string(
                destination_blob_name=blob_name,
                data=json.dumps(record),
            )

            gs_uri = f"gs://{RAW_BUCKET}/{blob_name}"
            logger.info("Uploaded %s → %s", coin, gs_uri)

            # Respect CoinGecko rate limits between coins
            time.sleep(_API_CALL_DELAY)

            return True, gs_uri

        except Exception as exc:
            # Catch everything so one bad coin doesn't kill the pipeline.
            # The failed coin is returned to the caller for reporting.
            logger.error("FAILED to process coin '%s': %s", coin, exc, exc_info=True)
            return False, None

    def _already_loaded(self, coin_id: str, date_str: str) -> bool:
        """
        Returns True if a raw record for this coin + date already exists
        in BigQuery.  Prevents duplicate raw rows on re-runs.
        """
        try:
            query = f"""
                SELECT COUNT(*) AS row_count
                FROM `{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`
                WHERE coin_id = '{coin_id}'
                  AND DATE(ingestion_time) = '{date_str}'
            """
            result = list(self.bq_client.execute_query(query).result())
            return result[0].row_count > 0

        except Exception as exc:
            # If the check itself fails (e.g. table doesn't exist yet on
            # first ever run), log a warning and proceed with the load.
            logger.warning(
                "Idempotency check failed for %s — proceeding with load. Error: %s",
                coin_id, exc,
            )
            return False

    def _validate_payload(self, payload: dict, coin: str) -> None:
        """
        Guard against CoinGecko returning HTTP 200 with empty or missing
        arrays — happens for delisted or very low-volume coins.
        Raises ValueError so the coin is marked as failed and skipped.
        """
        required_keys = ["prices", "market_caps", "total_volumes"]
        missing = [k for k in required_keys if not payload.get(k)]

        if missing:
            raise ValueError(
                f"CoinGecko response for '{coin}' is missing or empty "
                f"fields: {missing}"
            )

        logger.info(
            "Validated %s — prices: %d points, volumes: %d points",
            coin,
            len(payload["prices"]),
            len(payload["total_volumes"]),
        )

    def _load_to_bigquery(self, uris: list[str]) -> None:
        """
        Batch-load all successfully uploaded GCS files into the BigQuery
        raw table in a single load job (cheaper + faster than one job per file).
        """
        table_id = f"{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            # WRITE_APPEND is safe here because _already_loaded() guards
            # against duplicates before we upload.
            write_disposition="WRITE_APPEND",
            # Don't fail the job if CoinGecko adds a new field we don't know about yet.
            ignore_unknown_values=True,
        )

        logger.info(
            "Loading %d file(s) into BigQuery → %s", len(uris), table_id
        )

        load_job = self.bq_client.load_json_to_table(
            uri=uris,          # BQ accepts a list of gs:// URIs in one job
            table_id=table_id,
            job_config=job_config,
        )

        logger.info(
            "BigQuery raw load complete — %s rows written.",
            getattr(load_job, "output_rows", "unknown"),
        )

    def _run_transformations(self) -> None:
        """
        Run staging_merge.sql then curated_daily_metrics.sql in order.
        Uses pathlib paths resolved relative to this file — works on
        Windows dev machines and Linux GCP environments equally.
        """
        steps = [
            ("Staging merge",         _STAGING_MERGE_SQL),
            ("Curated daily metrics", _CURATED_MERGE_SQL),
        ]

        for label, sql_path in steps:
            logger.info("Running SQL transform: %s", label)
            try:
                query = sql_path.read_text(encoding="utf-8")
                self.bq_client.execute_query(query)
                logger.info("Completed: %s ✓", label)

            except FileNotFoundError:
                logger.error(
                    "SQL file not found: %s  "
                    "(check that the path exists relative to this file)",
                    sql_path,
                )
                raise

            except Exception as exc:
                logger.error("SQL transform FAILED [%s]: %s", label, exc, exc_info=True)
                raise
