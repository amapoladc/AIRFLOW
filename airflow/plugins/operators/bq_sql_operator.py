
from __future__ import annotations
from airflow.models import BaseOperator
from airflow.utils.context import Context
from google.cloud import bigquery
import os

class BigQuerySQLOperator(BaseOperator):
    template_fields = ("credentials","project","sql")
    def __init__(self, *, credentials: str | None, project: str | None, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.credentials = credentials; self.project = project; self.sql = sql
    def execute(self, context: Context):
        if self.credentials:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials
        client = bigquery.Client(project=self.project)
        # Allow multiple statements separated by ';'
        for stmt in [s.strip() for s in self.sql.split(';') if s.strip()]:
            client.query(stmt).result()
