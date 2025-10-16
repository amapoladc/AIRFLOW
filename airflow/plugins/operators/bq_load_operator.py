from __future__ import annotations
from typing import Optional, Literal
from airflow.models import BaseOperator
from airflow.utils.context import Context
from utils.bq_service import BigQueryService

class BigQueryLoadOperator(BaseOperator):
    template_fields = ("credentials","project","table_id","action","gcs_uri","source_format")
    def __init__(
        self,
        *,
        credentials: str | None,
        project: str | None,
        table_id: str,
        action: str,
        gcs_uri: str,
        source_format: Literal["PARQUET","CSV"] = "PARQUET",  # NEW default
        write_disposition: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.credentials = credentials; self.project = project
        self.table_id = table_id; self.action = action; self.gcs_uri = gcs_uri
        self.source_format = source_format
        self.write_disposition = write_disposition

    def execute(self, context: Context):
        bq = BigQueryService(self.credentials, self.project)
        bq.manage_table(self.table_id, self.action)
        bq.load_from_gcs(
            gcs_uri=self.gcs_uri,
            table_id=self.table_id,
            source_format=self.source_format,
            write_disposition=self.write_disposition,
        )
