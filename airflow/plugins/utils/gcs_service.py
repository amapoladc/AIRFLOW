from __future__ import annotations
import os
import tempfile
from datetime import datetime
from typing import Tuple

import pandas as pd  # NEW (needs pyarrow installed)
from google.cloud import storage
from utils.custom_logger import CustomLogger

class GCSService:
    def __init__(self, bucket: str, credentials: str | None = None):
        if credentials:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)

    def _find_latest(self, local_dir: str, input_pattern: str) -> Tuple[str, float]:
        latest_file, latest_mtime = None, 0.0
        for root, _, files in os.walk(local_dir):
            for f in files:
                if input_pattern in f:
                    p = os.path.join(root, f)
                    m = os.path.getmtime(p)
                    if m > latest_mtime:
                        latest_mtime, latest_file = m, p
        return latest_file, latest_mtime

    def _to_parquet(self, src_path: str) -> str:
        """
        Reads CSV *or* Parquet and writes a cleaned Parquet to a temp path.
        - If input is already parquet, just copy to temp parquet path.
        """
        _, ext = os.path.splitext(src_path.lower())
        tmpdir = tempfile.mkdtemp(prefix="gcs_parquet_")
        out_path = os.path.join(tmpdir, "payload.parquet")

        if ext == ".parquet":
            # Just copy as-is to temp parquet
            import shutil
            shutil.copy2(src_path, out_path)
            return out_path

        # Assume CSV otherwise; tweak read_csv args to your real CSV shape
        df = pd.read_csv(src_path)  # add sep=";" / encoding / dtype as needed
        # Write with pyarrow
        df.to_parquet(out_path, index=False)
        return out_path

    def upload_raw_and_backup(self, local_dir: str, input_pattern: str, output_basename: str) -> str:
        """
        NEW: Converts the latest matching file to Parquet and uploads:
          - raw/RAW_<basename>.parquet
          - backup/<basename>_YYYY-MM-DD.parquet
        Returns the local *parquet* path that was uploaded.
        """
        latest_file, _ = self._find_latest(local_dir, input_pattern)
        if not latest_file:
            CustomLogger.emit(4, "upload_to_gcs", "PCGCS", input_pattern, "GCS", "True", "No matching file")
            raise FileNotFoundError("No matching file")

        parquet_path = self._to_parquet(latest_file)
        today = datetime.now().strftime("%Y-%m-%d")

        raw_blob = self.bucket.blob(f"raw/RAW_{output_basename}.parquet")
        raw_blob.upload_from_filename(parquet_path)

        bkp_blob = self.bucket.blob(f"backup/{output_basename}_{today}.parquet")
        bkp_blob.upload_from_filename(parquet_path)

        CustomLogger.emit(4, "upload_to_gcs", "PCGCS", parquet_path, "GCS", "False", "RAW+BACKUP Parquet subidos")
        return parquet_path
