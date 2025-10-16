# /opt/airflow/plugins/operators/merge_process_operator.py
from __future__ import annotations

import os, sys, glob, re
from typing import List, Tuple
from datetime import datetime

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.context import Context

# --- shim de import para Airflow plugins (evita "No module named 'plugins') ---
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
for p in (AIRFLOW_HOME, os.path.join(AIRFLOW_HOME, "plugins")):
    if p not in sys.path:
        sys.path.insert(0, p)
# ------------------------------------------------------------------------------

# Import tolerante al layout de plugins
try:
    from plugins.utils.custom_logger import CustomLogger
except ImportError:
    from utils.custom_logger import CustomLogger  # fallback si existe legacy


class MergeProcessOperator(BaseOperator):
    """
    Une el Calls Detail (SIT_LZ_CALLDETAIL.csv) con los CSV de campañas (Generali / Generali Alt).
    - Acepta rutas directas en config["campaign_files"]; si no, usa glob tolerante
      para encontrar archivos con variantes "generali"/"generalli" y "alt".
    - Si falta el CSV base, continúa con ALT (emite warning).
    """
    template_fields = ("config",)

    def __init__(self, *, config: dict, **kwargs):
        super().__init__(**kwargs)
        self.config = config or {}

    # ----------------------- helpers -----------------------

    def _glob_many(self, base: str, patterns) -> List[str]:
        """
        Case-insensitive glob:
        - Convierte cada patrón con * y ? a regex
        - Compara contra los nombres del dir ignorando mayúsculas/minúsculas
        - Devuelve rutas únicas ordenadas por mtime desc
        """
        if isinstance(patterns, str):
            patterns = [patterns]
        try:
            entries = os.listdir(base)
        except FileNotFoundError:
            return []

        regexes = []
        for pat in patterns:
            pat = pat.strip()
            if not pat:
                continue
            # a regex, pero insensible a mayúsculas
            rx = '^' + re.escape(pat).replace(r'\*', '.*').replace(r'\?', '.') + '$'
            regexes.append(re.compile(rx, flags=re.IGNORECASE))

        hits = set()
        for name in entries:
            for rx in regexes:
                if rx.match(name):
                    hits.add(os.path.join(base, name))
                    break

        return sorted(hits, key=lambda p: os.path.getmtime(p), reverse=True)

    @staticmethod
    def _normcols(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns.astype(str)
              .str.strip()
              .str.replace(r'\s+', '_', regex=True)
              .str.replace(r'[^\w]+', '_', regex=True)
              .str.lower()
        )
        return df

    @staticmethod
    def _ensure_uniqueid(df: pd.DataFrame, label: str) -> None:
        if 'uniqueid' in df.columns:
            return
        for c in ('unique_id', 'id_unico', 'id'):
            if c in df.columns:
                df.rename(columns={c: 'uniqueid'}, inplace=True)
                return
        raise KeyError(f"[{label}] No se encontró columna 'uniqueid' (ni alias).")

    @staticmethod
    def _cast_phone_if_exists(df: pd.DataFrame) -> None:
        for c in ('phone', 'phone_number', 'telefono', 'telefone', 'phone_'):
            if c in df.columns:
                df[c] = df[c].astype(str)
                break

    # ----------------------- execute -----------------------

    def execute(self, context: Context):
        cfg = self.config
        local_path = cfg.get('local_path', '/opt/airflow/data/Extracted_generali')
        os.makedirs(local_path, exist_ok=True)

        final_file = cfg.get('final_file_name', 'SIT_BK_FINAL_MERGE.csv')
        if not final_file.endswith('.csv'):
            final_file += '.csv'
        out_path = os.path.join(local_path, final_file)

        # Calls detail
        calls_detail_path = cfg.get('calls_detail_path') or os.path.join(local_path, 'SIT_LZ_CALLDETAIL.csv')
        if not os.path.exists(calls_detail_path):
            try:
                listing = os.listdir(local_path)
            except Exception:
                listing = []
            raise FileNotFoundError(
                f"No existe {calls_detail_path}. Contenido del directorio (sample): {listing[:50]}"
            )

        # Descubrir campañas: usar rutas directas si están, si no glob tolerante
        campaign_files = [p for p in cfg.get("campaign_files", []) if isinstance(p, str) and os.path.exists(p)]
        if not campaign_files:
            # Busca TODO lo que se parezca a generali/generalli (con y sin alt)
            campaign_files = self._glob_many(local_path, [
                'generali*.csv', 'generalli*.csv',
                '*generali*.csv', '*generalli*.csv',
            ])

        # Clasifica base vs alt
        def is_alt(path: str) -> bool:
            bn = os.path.basename(path).lower()
            return any(k in bn for k in (" alt", "_alt", "-alt", "alt."))

        primaries = [p for p in campaign_files if not is_alt(p)]
        alts      = [p for p in campaign_files if is_alt(p)]

        # Selecciona el más reciente de cada tipo (si existiera)
        gen_path = primaries[0] if primaries else None
        alt_path = alts[0] if alts else None

        # Logging de diagnóstico
        try:
            self.log.info("Merge local_path: %s", local_path)
            self.log.info("Calls detail path: %s", calls_detail_path)
            self.log.info("Campaign base (mas reciente): %s", gen_path)
            self.log.info("Campaign alt  (mas reciente): %s", alt_path)
            self.log.info("Campaign files (all candidates): %s", campaign_files[:10])
            self.log.info("Dir listing (sample): %s", os.listdir(local_path)[:50])
        except Exception:
            pass

        # Si no hay base pero sí ALT, continuar con ALT (warning)
        if not gen_path and alt_path:
            self.log.warning("No se encontró CSV base (Generali/Generalli). Continuando SOLO con ALT.")
        # Si no hay ninguno, error duro
        if not gen_path and not alt_path:
            raise FileNotFoundError(
                "No se encontraron CSV de campañas (ni base ni alt). "
                f"Busqué en {local_path} y en config['campaign_files']."
            )

        # --- LEER CSVs ---

        # Calls detail
        df_calls = pd.read_csv(calls_detail_path, dtype=str, engine='python', on_bad_lines='skip')
        df_calls = self._normcols(df_calls)
        self._ensure_uniqueid(df_calls, "calls_detail")
        self._cast_phone_if_exists(df_calls)

        # Campañas (1..2)
        dfs_camps: List[Tuple[str, pd.DataFrame]] = []

        if gen_path:
            df1 = pd.read_csv(gen_path, header=1, dtype=str, engine='python', on_bad_lines='skip')
            df1 = self._normcols(df1)
            self._ensure_uniqueid(df1, "campaña_base")
            self._cast_phone_if_exists(df1)
            df1['source'] = 'Generali'  # normalizamos etiqueta
            dfs_camps.append(("Generali", df1))

        if alt_path:
            df2 = pd.read_csv(alt_path, header=1, dtype=str, engine='python', on_bad_lines='skip')
            df2 = self._normcols(df2)
            self._ensure_uniqueid(df2, "campaña_alt")
            self._cast_phone_if_exists(df2)
            df2['source'] = 'GeneraliAlt'
            dfs_camps.append(("GeneraliAlt", df2))

        if not dfs_camps:
            raise FileNotFoundError("No hay CSVs de campañas para procesar (ni base ni alt).")

        # Concat campañas
        df_camps = pd.concat([df for _, df in dfs_camps], ignore_index=True)

        # Tipos comparables para merge
        df_calls['uniqueid'] = df_calls['uniqueid'].astype(str)
        df_camps['uniqueid'] = df_camps['uniqueid'].astype(str)

        # Merge
        dff = df_calls.merge(df_camps, on='uniqueid', how='inner')

        # Renombres (versión lower/underscore)
        rename_map = {
            'no._agent': 'agent_number',
            'agent': 'agent_name',
            'start_time': 'start_time',
            'end_time': 'end_time',
            'duration': 'call_duration',
            'duration_wait': 'wait_duration',
            'queue': 'call_queue',
            'type': 'call_type',
            'phone_x': 'phone_number_x',
            'phone_y': 'phone_number_y',
            'transfer': 'call_transfer',
            'status': 'call_status',
            'did': 'direct_inward_dialing',
            'uniqueid': 'uniqueid',
            'status_call': 'call_status_detail',
            'agente': 'agent',
            'date_&_time': 'datetime',
            'duration(seg)': 'duration_seconds',
            'cedula/ruc': 'id_number',
            'first_name': 'first_name',
            'last_name': 'last_name',
            'seleccione_la_cartera': 'portfolio',
            'tipo_de_atención': 'attention_type',
            'estado': 'state',
            'motivo': 'reason',
            'source': 'call_source',
        }
        to_rename = {k: v for k, v in rename_map.items() if k in dff.columns and k != v}
        if to_rename:
            dff = dff.rename(columns=to_rename)

        # DID a número si existe (permite nulos)
        if 'direct_inward_dialing' in dff.columns:
            dff['direct_inward_dialing'] = (
                pd.to_numeric(dff['direct_inward_dialing'].replace('-', pd.NA), errors='coerce')
                  .astype('Int64')
            )

        dff['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Escribe salida
        dff.to_csv(out_path, index=False, encoding='utf-8')

        CustomLogger.emit(
            3, 'merge_and_process_calls', 'PC_MERGE', out_path, 'CSV', 'False',
            f'Final creado: {out_path}'
        )
        self.log.info("Archivo final creado: %s (rows=%s, cols=%s)", out_path, len(dff), len(dff.columns))
        return out_path
