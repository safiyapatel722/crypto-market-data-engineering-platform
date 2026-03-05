# src/infrastructure/bigquery_client.py

from typing import Union

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

from configs.settings import PROJECT_ID
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BigQueryClient:
    """
    Thin wrapper around the Google Cloud BigQuery SDK.

    Fixes applied vs original:
      - Error handling on both execute_query() and load_json_to_table()
      - Checks job.errors after load — BQ jobs can 'succeed' with partial errors
      - Logs job ID and row counts after every operation (essential for debugging)
      - load_json_to_table() accepts a list of URIs for batch loading
        (one BQ job for N files — cheaper and faster than N separate jobs)
    """

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)

    # ── Public methods ─────────────────────────────────────────────────────────

    def execute_query(self, query: str) -> bigquery.QueryJob:
        """
        Execute a BigQuery SQL query and block until complete.

        Args:
            query: SQL string to execute (e.g. a MERGE or DDL statement).

        Returns:
            The completed QueryJob object.

        Raises:
            GoogleAPIError: On any BigQuery API failure.
            RuntimeError:   If the job reports errors after completing.
        """
        logger.info("Executing BigQuery query (%d chars)...", len(query))

        try:
            job = self.client.query(query)
            job.result()  # blocks until the job finishes

            # FIX: Always check job.errors. A query job can return without
            # raising an exception but still carry error details that would
            # silently corrupt downstream data.
            if job.errors:
                raise RuntimeError(
                    f"BigQuery query job completed with errors: {job.errors}"
                )

            logger.info(
                "Query complete — job_id=%s  bytes_processed=%s",
                job.job_id,
                getattr(job, "total_bytes_processed", "unknown"),
            )
            return job

        except GoogleAPIError as exc:
            logger.error("BigQuery query failed: %s", exc, exc_info=True)
            raise

    def load_json_to_table(
        self,
        uri: Union[str, list],
        table_id: str,
        job_config: bigquery.LoadJobConfig,
    ) -> bigquery.LoadJob:
        """
        Load one or more GCS JSON files into a BigQuery table in a single job.

        Args:
            uri:        A single gs:// URI string, or a list of gs:// URI strings.
                        Passing a list loads all files in one job —
                        much cheaper and faster than one job per file.
            table_id:   Fully-qualified BQ table e.g. "project.dataset.table".
            job_config: LoadJobConfig (source format, write disposition, etc.)

        Returns:
            The completed LoadJob object.

        Raises:
            GoogleAPIError: On any BigQuery API failure.
            RuntimeError:   If the load job reports errors after completing.
        """
        # Normalise to list so the rest of the method is uniform.
        uris = [uri] if isinstance(uri, str) else uri

        logger.info(
            "Starting BQ load — %d file(s) → %s", len(uris), table_id
        )

        try:
            load_job = self.client.load_table_from_uri(
                uris,
                table_id,
                job_config=job_config,
            )
            load_job.result()  # blocks until the job finishes

            # FIX: Always check load_job.errors. BigQuery can report success
            # (no exception raised) while still carrying per-row errors.
            # Without this check those errors are silently swallowed.
            if load_job.errors:
                raise RuntimeError(
                    f"BQ load job completed with errors: {load_job.errors}"
                )

            logger.info(
                "Load complete — job_id=%s  rows_written=%s  table=%s",
                load_job.job_id,
                getattr(load_job, "output_rows", "unknown"),
                table_id,
            )
            return load_job

        except GoogleAPIError as exc:
            logger.error(
                "BigQuery load failed for '%s': %s", table_id, exc, exc_info=True
            )
            raise