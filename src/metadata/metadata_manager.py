# src/metadata/metadata_manager.py

from datetime import date
from typing import Optional

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from configs.settings import PROJECT_ID, METADATA_DATASET, METADATA_TABLE
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MetadataManager:
    """
    Reads and writes pipeline run state to a BigQuery metadata table.

    Tracks the last successfully processed date per pipeline so incremental
    pipelines can resume from exactly where they left off.

    Expected BigQuery table schema:
        pipeline_name        STRING     (primary key)
        last_processed_date  DATE
        updated_at           TIMESTAMP

    Fixes applied vs original:
      - FIX: Dataset/table names now come from settings.py constants
             (was hardcoded to "metadata.pipeline_state" which didn't match
              METADATA_DATASET="platform_metadata" in settings.py — they
              pointed to two different tables and would never align)
      - FIX: Parameterized queries throughout — eliminates SQL injection risk
      - FIX: ORDER BY updated_at DESC on SELECT so LIMIT 1 is deterministic
             (original had no ORDER BY — returned an arbitrary row if there
              were multiple entries for the same pipeline name)
      - FIX: Specific GoogleAPIError handling instead of bare Exception
      - FIX: Full type hints on all public methods
    """

    def __init__(self, project_id: str = PROJECT_ID):
        self.client   = bigquery.Client(project=project_id)

        # FIX: Use constants from settings.py instead of hardcoded strings.
        self.table_id = f"{project_id}.{METADATA_DATASET}.{METADATA_TABLE}"

        logger.info("MetadataManager — table: %s", self.table_id)

    # ── Public methods ─────────────────────────────────────────────────────────

    def get_last_processed_date(self, pipeline_name: str) -> Optional[date]:
        """
        Fetch the last successfully processed date for the given pipeline.

        Args:
            pipeline_name: Unique pipeline identifier,
                           e.g. "crypto_incremental_pipeline".

        Returns:
            A datetime.date if a previous run exists, None on first run.

        Raises:
            GoogleAPIError: On any BigQuery API failure.
        """
        # FIX: Parameterized query — never interpolate strings directly into SQL.
        # Even though pipeline_name is internal, this is the correct habit and
        # protects against accidental injection if names ever come from config files.
        query = f"""
            SELECT last_processed_date
            FROM `{self.table_id}`
            WHERE pipeline_name = @pipeline_name
            ORDER BY updated_at DESC
            LIMIT 1
        """
        # ↑ FIX: ORDER BY updated_at DESC added so LIMIT 1 always returns the
        # most recent entry — the original returned an arbitrary row.

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
            ]
        )

        try:
            result = self.client.query(query, job_config=job_config).result()

            for row in result:
                logger.info(
                    "Last processed date for '%s': %s",
                    pipeline_name, row.last_processed_date,
                )
                return row.last_processed_date

            # No rows = first ever run for this pipeline. This is expected
            # and not an error — the calling pipeline handles None gracefully.
            logger.warning(
                "No metadata found for pipeline '%s' — treating as first run.",
                pipeline_name,
            )
            return None

        except GoogleAPIError as exc:
            logger.error(
                "Failed to fetch last processed date for '%s': %s",
                pipeline_name, exc, exc_info=True,
            )
            raise

    def update_last_processed_date(
        self,
        pipeline_name: str,
        processed_date: date,
    ) -> None:
        """
        Upsert the last processed date for the given pipeline.
        Uses MERGE so it works correctly for both the first run (INSERT)
        and all subsequent runs (UPDATE).

        Args:
            pipeline_name:  Unique pipeline identifier.
            processed_date: The last date that was fully and successfully processed.

        Raises:
            GoogleAPIError: On any BigQuery API failure.
        """
        # FIX: Both values are parameterized.
        # The original used f-strings for both, which is a SQL injection risk
        # and also fragile when processed_date is a date object (not a string).
        query = f"""
            MERGE `{self.table_id}` T
            USING (
                SELECT
                    @pipeline_name  AS pipeline_name,
                    @processed_date AS last_processed_date,
                    CURRENT_TIMESTAMP() AS updated_at
            ) S
            ON T.pipeline_name = S.pipeline_name

            WHEN MATCHED THEN
                UPDATE SET
                    last_processed_date = S.last_processed_date,
                    updated_at          = S.updated_at

            WHEN NOT MATCHED THEN
                INSERT (pipeline_name, last_processed_date, updated_at)
                VALUES (S.pipeline_name, S.last_processed_date, S.updated_at)
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("pipeline_name",  "STRING", pipeline_name),
                # str() handles both date objects and strings safely
                ScalarQueryParameter("processed_date", "DATE",   str(processed_date)),
            ]
        )

        try:
            self.client.query(query, job_config=job_config).result()

            logger.info(
                "Metadata updated — pipeline='%s'  last_processed_date=%s",
                pipeline_name, processed_date,
            )

        except GoogleAPIError as exc:
            logger.error(
                "Failed to update metadata for '%s': %s",
                pipeline_name, exc, exc_info=True,
            )
            raise