from __future__ import annotations
from airflow.models import BaseOperator
from airflow.utils.context import Context
from utils.gcs_service import GCSService

class GCSUploadOperator(BaseOperator):
    template_fields = ("bucket","credentials","local_dir","input_pattern","output_basename")
    def __init__(self, *, bucket: str, credentials: str | None, local_dir: str, input_pattern: str, output_basename: str, **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket; self.credentials = credentials
        self.local_dir = local_dir; self.input_pattern = input_pattern; self.output_basename = output_basename
    def execute(self, context: Context):
        svc = GCSService(self.bucket, self.credentials)
        return svc.upload_raw_and_backup(self.local_dir, self.input_pattern, self.output_basename)
