from google.cloud import bigquery
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MetadataManager:

    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self.table_id = f"{project_id}.metadata.pipeline_state"

    def get_last_processed_date(self, pipeline_name: str):

        try:
            query = f"""
            SELECT last_processed_date
            FROM `{self.table_id}`
            WHERE pipeline_name = '{pipeline_name}'
            LIMIT 1
            """

            result = self.client.query(query).result()

            for row in result:
                logger.info("Last processed date found: %s", row.last_processed_date)
                return row.last_processed_date

            logger.warning("No previous run metadata found for pipeline: %s", pipeline_name)
            return None

        except Exception as e:
            logger.error("Failed to fetch last processed date: %s", str(e))
            raise

    def update_last_processed_date(self, pipeline_name: str, processed_date):

        try:
            query = f"""
            MERGE `{self.table_id}` T
            USING (
                SELECT
                    '{pipeline_name}' AS pipeline_name,
                    DATE('{processed_date}') AS last_processed_date,
                    CURRENT_TIMESTAMP() AS updated_at
            ) S
            ON T.pipeline_name = S.pipeline_name

            WHEN MATCHED THEN
              UPDATE SET
                last_processed_date = S.last_processed_date,
                updated_at = S.updated_at

            WHEN NOT MATCHED THEN
              INSERT (pipeline_name, last_processed_date, updated_at)
              VALUES (S.pipeline_name, S.last_processed_date, S.updated_at)
            """

            self.client.query(query).result()

            logger.info(
                "Metadata updated successfully for pipeline %s with date %s",
                pipeline_name,
                processed_date
            )

        except Exception as e:
            logger.error("Failed to update metadata: %s", str(e))
            raise