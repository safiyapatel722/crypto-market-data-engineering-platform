# src/infrastructure/bigquery_client.py

from google.cloud import bigquery
from configs.settings import PROJECT_ID

class BigQueryClient:

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)

    def execute_query(self, query: str):
        job = self.client.query(query)
        job.result()
        return job

    def load_json_to_table(self, uri: str, table_id: str, job_config):
        load_job = self.client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )
        load_job.result()
        return load_job