# /opt/airflow/plugins/operators/selenium_download_operator.py
from __future__ import annotations

from typing import Dict, Any
from datetime import datetime, timedelta
import os

from airflow.models import BaseOperator
from airflow.utils.context import Context

# Imports tolerantes al loader de plugins
try:
    from utils.selenium_service import SeleniumDownloader, fmt_date_en_dd_M_yyyy
except ImportError:
    from plugins.utils.selenium_service import SeleniumDownloader, fmt_date_en_dd_M_yyyy
from plugins.utils.selenium_service import SeleniumDownloader, fmt_date_en_dd_M_yyyy


class SeleniumDownloadOperator(BaseOperator):
    """
    Operator para descargar reportes desde el portal Virfon (History -> Calls Detail / Campaigns)
    usando Selenium en modo headless. Devuelve la(s) ruta(s) final(es) de archivo descargado.
    """
    template_fields = ("config", "mode")

    def __init__(self, *, config: Dict[str, Any], mode: str, **kwargs):
        """
        Args:
            config: Diccionario de configuración. Ejemplo:
              {
                "base_url": "https://memorialbr.virfon.com/",
                "local_path": "/opt/airflow/data/Extracted_generali",
                "user_name": "user",
                "user_password": "pass",
                "timeDelta": 1,
                "outputFileName": "SIT_LZ_CALLDETAIL.csv",
                "campaign_names": ["Generalli", "Generali alt"],
                "driver_path": "/usr/bin/chromedriver"
              }
            mode: 'calls_detail' | 'campaigns'
        """
        super().__init__(**kwargs)
        if mode not in ("calls_detail", "campaigns"):
            raise ValueError("mode inválido; usa 'calls_detail' o 'campaigns'")
        self.config = config
        self.mode = mode  # 'calls_detail' | 'campaigns'

    def execute(self, context: Context):
        cfg = self.config or {}
        base_url = cfg.get('base_url', 'https://memorialbr.virfon.com/')
        download_dir = cfg.get('local_path', '/opt/airflow/data/Extracted_generali')
        os.makedirs(download_dir, exist_ok=True)

        driver_path = cfg.get("driver_path", "/usr/bin/chromedriver")
        user = cfg.get('user_name', '')
        password = cfg.get('user_password', '')

        # timeDelta = 1 => “ayer” por defecto
        days_delta = int(cfg.get('timeDelta', 1))
        when = datetime.now() - timedelta(days=days_delta)
        # Formato exacto para jQuery UI: '08 Oct 2025'
        report_date = fmt_date_en_dd_M_yyyy(when)

        # No logeamos password
        self.log.info("Descarga Selenium mode=%s base=%s dir=%s fecha=%s",
                      self.mode, base_url, download_dir, report_date)

        with SeleniumDownloader(base_url, download_dir, driver_path=driver_path) as sd:
            sd.login(user, password)

            if self.mode == 'calls_detail':
                path = sd.download_calls_detail(report_date)

                # Renombra al nombre esperado si es posible
                final_path = os.path.join(download_dir, cfg.get('outputFileName', 'SIT_LZ_CALLDETAIL.csv'))
                try:
                    # Si final_path existe, lo reemplaza (mueve atómico)
                    os.replace(path, final_path)
                except Exception as e:
                    self.log.warning("No se pudo renombrar '%s' -> '%s' (%s). Devolviendo ruta original.",
                                     path, final_path, e)
                    final_path = path

                self.log.info("Calls Detail descargado en: %s", final_path)
                return final_path

            # campaigns
            names = cfg.get('campaign_names', ['Generalli', 'Generali alt'])
            self.log.info("Descargando campañas: %s", names)
            out_list = sd.download_campaigns(names)
            self.log.info("Campañas descargadas: %s", out_list)
            return out_list
