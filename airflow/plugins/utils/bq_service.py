from __future__ import annotations
import os
from typing import Literal, Optional

from google.cloud import bigquery
from utils.custom_logger import CustomLogger

class BigQueryService:
    def __init__(self, credentials: str | None = None, project: str | None = None):
        if credentials:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials
        self.client = bigquery.Client(project=project)

    def manage_table(self, table_id: str, action: str):
        action = (action or "").upper()
        if action == "TRUNCATE":
            self.client.query(f"DELETE FROM `{table_id}` WHERE TRUE").result()
        elif action == "DROP":
            self.client.delete_table(table_id, not_found_ok=True)
        elif action == "INSERT_INTO":
            pass
        else:
            raise ValueError("Invalid action")
        CustomLogger.emit(5, "manage_table", "PCBQ", table_id, "BIGQUERY", "False", f"ACTION: {action}")

    def load_from_gcs(
        self,
        gcs_uri: str,
        table_id: str,
        source_format: Literal["PARQUET", "CSV"] = "PARQUET",
        write_disposition: Optional[str] = None,
    ):
        """
        Generic loader. Default PARQUET.
        """
        if source_format == "PARQUET":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition or bigquery.WriteDisposition.WRITE_APPEND,
            )
        elif source_format == "CSV":
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition=write_disposition or bigquery.WriteDisposition.WRITE_APPEND,
            )
        else:
            raise ValueError("Unsupported source_format")

        job = self.client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        job.result()
        t = self.client.get_table(table_id)
        CustomLogger.emit(5, "load_gcs", "PCBQ", table_id, "BIGQUERY", "False", f"Rows: {t.num_rows}")
