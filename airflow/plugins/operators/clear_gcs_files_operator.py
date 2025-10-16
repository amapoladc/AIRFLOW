
from __future__ import annotations
from airflow.models import BaseOperator
from airflow.utils.context import Context
from google.cloud import storage
import os

class ClearGCSFilesOperator(BaseOperator):
    template_fields = ("credentials","bucket","folder","files")
    def __init__(self, *, credentials: str | None, bucket: str, folder: str, files: list[str], **kwargs):
        super().__init__(**kwargs)
        self.credentials = credentials; self.bucket = bucket; self.folder = folder; self.files = files
    def execute(self, context: Context):
        if self.credentials:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials
        client = storage.Client(); b = client.bucket(self.bucket)
        for f in self.files:
            p = f"{self.folder}/{f}"
            blob = b.blob(p)
            if blob.exists(): blob.delete(); self.log.info(f"Deleted: {p}")
            else: self.log.info(f"Not found: {p}")
