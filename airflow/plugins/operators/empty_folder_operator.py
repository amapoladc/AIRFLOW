
from __future__ import annotations
import os, shutil
from airflow.models import BaseOperator
from airflow.utils.context import Context

class EmptyFolderOperator(BaseOperator):
    template_fields = ("path",)
    def __init__(self, *, path: str, **kwargs):
        super().__init__(**kwargs)
        self.path = path
    def execute(self, context: Context):
        p = self.path
        if not os.path.exists(p):
            self.log.info(f"Carpeta no existe: {p}"); return
        for item in os.listdir(p):
            fp = os.path.join(p,item)
            try:
                if os.path.isfile(fp) or os.path.islink(fp): os.unlink(fp)
                elif os.path.isdir(fp): shutil.rmtree(fp)
            except Exception as e:
                self.log.warning(f"No se pudo eliminar {fp}: {e}")
        self.log.info(f"Carpeta vaciada: {p}")
