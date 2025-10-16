# /opt/airflow/dags/etl_generali_oop.py
from __future__ import annotations

# Ensure /opt/airflow is importable so "plugins" is visible
import os
import sys
import shutil
from pathlib import Path
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

from datetime import datetime, timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator  # (lo dejamos por si lo usas en otras partes)

# --- Plugins (under /opt/airflow/plugins)
from plugins.utils.custom_logger import CustomLogger
from plugins.utils.params_loader import ParamReader

from plugins.operators.selenium_download_operator import SeleniumDownloadOperator
from plugins.operators.merge_process_operator import MergeProcessOperator
from plugins.operators.gcs_upload_operator import GCSUploadOperator
from plugins.operators.bq_load_operator import BigQueryLoadOperator
from plugins.operators.bq_sql_operator import BigQuerySQLOperator
from plugins.operators.clear_gcs_files_operator import ClearGCSFilesOperator
from plugins.operators.empty_folder_operator import EmptyFolderOperator
from plugins.operators.dbt_operator import DbtOperator   # custom operator actualizado

TAGS = ["PythonDataFlow"]
DAG_ID = "ETL_GENERALI_OOP"
local_tz = pendulum.timezone("America/Sao_Paulo")


@dag(
    dag_id=DAG_ID,
    description="Refactor OOP ETL Generali",
    schedule="0 5 * * *",  # todos los días 05:00
    catchup=False,
    start_date=datetime(2025, 2, 24, tzinfo=local_tz),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=0)},
    tags=TAGS,
    
)
def etl_generali_oop():
    # -----------------------------
    # Helpers como tareas ligeras
    # -----------------------------
    @task(trigger_rule="all_success")
    def reset_logger():
        CustomLogger.reset()
        return "ok"

    @task(trigger_rule="all_success")
    def read_params() -> list[dict] | dict:
        params_file = os.getenv("PARAMS_FILE", "/opt/airflow/params/generali.yaml")
        defaults_file = os.getenv("PARAMS_DEFAULTS", "") or None
        return ParamReader.read_yaml(params_file, defaults_file)

    @task(trigger_rule="all_success")
    def by_prefix(data: list[dict] | dict, prefix: str) -> list[dict] | dict:
        """
        Filtra el YAML parametrizado por prefijo (PCW/PM1/PCGCS/PCBQ/DBT)
        y devuelve lista de dicts o dict (según entrada).
        """
        return ParamReader.by_prefix(data, prefix)

    @task
    def adapt_gcs_kwargs(items: list[dict]) -> list[dict]:
        """Adapta los dicts de PCGCS al kwargs requerido por GCSUploadOperator."""
        return [
            dict(
                bucket=i["bucket_name"],
                credentials="/opt/airflow/gcp/dbt_test.json",
                local_dir=i["local_path"],
                input_pattern=i["inputFileName"],
                output_basename=i["outputFileName"],
            )
            for i in items
        ]

    @task
    def adapt_bq_kwargs(items: list[dict]) -> list[dict]:
        """Adapta los dicts de PCBQ al kwargs requerido por BigQueryLoadOperator."""
        out: list[dict] = []
        for i in items:
            out.append(
                dict(
                    credentials="/opt/airflow/gcp/dbt_test.json",
                    project=i["project_id"],
                    table_id=f"{i['project_id']}.{i['dataset_name']}.{i['table_name']}",
                    action=i["truncate_drop_into"],  # TRUNCATE | DROP | INSERT_INTO
                    gcs_uri=f"gs://{i['bucket_name']}/{i['source_objects']}",
                )
            )
        return out

    # --------------
    # DBT prep en UNA sola función
    # --------------
    @task(multiple_outputs=True)
    def prepare_dbt(params_all: list[dict] | dict) -> dict:
        """
        - Extrae bloque DBT (adm. dict o list[dict])
        - Resuelve placeholders simples ${...}
        - Crea /home/airflow/.dbt y copia profiles.yml
        - Normaliza threads, full_refresh y selects por capa
        - Devuelve dict con: project_dir, profiles_dir, creds_path, threads, full_refresh, selects{...}, env
        """
        # 1) Extraer bloque DBT (aceptar dict o list[dict])
        dbt_block = ParamReader.by_prefix(params_all, "DBT")
        if isinstance(dbt_block, list):
            if not dbt_block:
                raise ValueError("No se encontró bloque 'DBT' en los parámetros.")
            dbt = dbt_block[0]
        elif isinstance(dbt_block, dict):
            dbt = dbt_block
        else:
            raise TypeError(f"Bloque DBT inválido: {type(dbt_block)}")

        def _is_placeholder(v: str) -> bool:
            return isinstance(v, str) and "${" in v

        # 2) Campos base
        project_dir = dbt.get("project_dir") or "/opt/airflow/dbt/GENERALI"
        profiles_src = dbt.get("profiles_src") or f"{project_dir}/profiles.yml"
        profiles_dir = "/home/airflow/.dbt"  # target fijo y conocido por dbt
        creds_path = dbt.get("credentials_path") or "/opt/airflow/gcp/dbt_test.json"
        threads = dbt.get("threads", 4)
        full_refresh = dbt.get("full_refresh", False)

        # 3) Resolver placeholders básicos: si viene ${...} forzar fallback
        if _is_placeholder(profiles_src):
            profiles_src = f"{project_dir}/profiles.yml"
        if _is_placeholder(creds_path) or not str(creds_path).strip():
            creds_path = "/opt/airflow/gcp/dbt_test.json"

        # 4) Asegurar profiles.yml en ~/.dbt
        Path(profiles_dir).mkdir(parents=True, exist_ok=True)
        src_path = Path(profiles_src)
        dst_path = Path(profiles_dir) / "profiles.yml"
        if src_path.is_file():
            shutil.copy2(src_path, dst_path)
        else:
            # Si no existe, lanza warning claro y crea archivo mínimo
            minimal = (
                "default:\n"
                "  target: dev\n"
                "  outputs:\n"
                "    dev:\n"
                "      type: bigquery\n"
                "      method: service-account\n"
                f"      keyfile: {creds_path}\n"
                "      project: memorialtechnologies-245614\n"
                "      dataset: generali\n"
                "      threads: 4\n"
            )
            with open(dst_path, "w", encoding="utf-8") as f:
                f.write(minimal)

        # 5) Normalizar tipos: threads (int) y full_refresh (bool)
        try:
            threads = int(threads)
        except Exception:
            threads = 4

        if isinstance(full_refresh, (str, int)):
            s = str(full_refresh).strip().lower()
            full_refresh = s in {"1", "true", "yes", "y", "t"}

        # 6) Normalizar selects por capa
        models = dbt.get("models", {}) or {}
        def _norm_select(v):
            if not v:
                return None
            if isinstance(v, list):
                out = [str(x).strip() for x in v if str(x).strip()]
                return out or None
            return [str(v).strip()] if str(v).strip() else None

        bronze_sel = _norm_select(models.get("bronze")) or ["tag:bronze"]
        silver_sel = _norm_select(models.get("silver")) or ["tag:silver"]
        gold_sel   = _norm_select(models.get("gold"))   or ["tag:gold"]

        # 7) ENV para el operador
        env = {
            "DBT_PROFILES_DIR": profiles_dir,
            "GOOGLE_APPLICATION_CREDENTIALS": creds_path,
            "PATH": os.environ.get("PATH", ""),
        }
        return {
            "project_dir": project_dir,
            "profiles_dir": profiles_dir,
            "credentials_path": creds_path,
            "threads": threads,
            "full_refresh": full_refresh,
            "bronze": bronze_sel,   # <- top-level
            "silver": silver_sel,   # <- top-level
            "gold": gold_sel,       # <- top-level
            "env": env,
        }

    # ---------------------------------
    # Limpieza previa (GCS, BQ, carpeta)
    # ---------------------------------
    with TaskGroup("clear_tasks", tooltip="Tareas de limpieza") as clear_tasks:
        clear_gcs = ClearGCSFilesOperator(
            task_id="clear_gcs",
            credentials="/opt/airflow/gcp/dbt_test.json",
            bucket="generali_call",
            folder="raw",
            files=["RAW_SIT_BK_FINAL_MERGE.csv"],
        )

        drop_tables = BigQuerySQLOperator(
            task_id="drop_bq_tables",
            credentials="/opt/airflow/gcp/dbt_test.json",
            project="memorialtechnologies-245614",
            sql=";".join(
                [
                    "DROP TABLE IF EXISTS `memorialtechnologies-245614.generali.DATA_WAREHAUSE_FINAL_MERGE`",
                    "DROP TABLE IF EXISTS `memorialtechnologies-245614.generali.call_data`",
                    "DROP TABLE IF EXISTS `memorialtechnologies-245614.generali.call_cleaning`",
                ]
            ),
        )

        empty_local = EmptyFolderOperator(
            task_id="empty_local", path="/opt/airflow/data/Extracted_generali"
        )

        clear_gcs >> drop_tables >> empty_local

    # ---------------------------
    # Lectura de parámetros única
    # ---------------------------
    params = read_params()

    # ---------------------------
    # Web scraping (PCW) mapeado
    # ---------------------------
    with TaskGroup("web_scraping_tasks", tooltip="Descargas via Selenium") as web_scraping:
        pcw = by_prefix(params, "PCW")

        selenium_calls = (
            SeleniumDownloadOperator.partial(
                task_id="download_calls_detail",
                mode="calls_detail",
            )
            .expand(config=pcw)
        )

        selenium_camps = (
            SeleniumDownloadOperator.partial(
                task_id="download_campaigns",
                mode="campaigns",
            )
            .expand(config=pcw)
        )

    # -------------------------------------
    # Pre-proceso / merge (PM1) mapeado
    # -------------------------------------
    with TaskGroup("preprocess_tasks", tooltip="Unión y normalización") as preprocess:
        pm1 = by_prefix(params, "PM1")
        merge_ops = MergeProcessOperator.partial(task_id="merge_and_process").expand(config=pm1)

    # -------------------------------------
    # Subida a GCS (PCGCS) mapeado
    # -------------------------------------
    with TaskGroup("gcs_tasks", tooltip="Carga a Google Cloud Storage") as gcs_tasks:
        pcgcs = by_prefix(params, "PCGCS")
        gcs_kwargs = adapt_gcs_kwargs(pcgcs)
        gcs_upload = GCSUploadOperator.partial(task_id="gcs_upload").expand_kwargs(gcs_kwargs)

    # -------------------------------------
    # Carga en BigQuery (PCBQ) mapeado
    # -------------------------------------
    with TaskGroup("bq_tasks", tooltip="Carga a BigQuery") as bq_tasks:
        pcbq = by_prefix(params, "PCBQ")
        bq_kwargs = adapt_bq_kwargs(pcbq)
        bq_load = BigQueryLoadOperator.partial(task_id="bq_load").expand_kwargs(bq_kwargs)

    # --------------
    # DBT (prep en una sola función)
    # --------------
    dbt_cfg = prepare_dbt(params)  # <-- ÚNICA tarea de preparación DBT

    with TaskGroup("bronze_layer") as bronze:
        run_bronze = DbtOperator(
            task_id="run_bronze",
            command="run",
            project_dir=dbt_cfg["project_dir"],
            profiles_dir=dbt_cfg["profiles_dir"],
            select=dbt_cfg["bronze"],
            env=dbt_cfg["env"],
            threads=dbt_cfg["threads"],
            full_refresh=dbt_cfg["full_refresh"],
        )

    with TaskGroup("silver_layer") as silver:
        run_silver = DbtOperator(
            task_id="run_silver",
            command="run",
            project_dir=dbt_cfg["project_dir"],
            profiles_dir=dbt_cfg["profiles_dir"],
            select=dbt_cfg["silver"],
            env=dbt_cfg["env"],
            threads=dbt_cfg["threads"],
            full_refresh=dbt_cfg["full_refresh"],
        )

    with TaskGroup("gold_layer") as gold:
        run_gold = DbtOperator(
            task_id="run_gold",
            command="run",
            project_dir=dbt_cfg["project_dir"],
            profiles_dir=dbt_cfg["profiles_dir"],
            select=dbt_cfg["gold"],
            env=dbt_cfg["env"],
            threads=dbt_cfg["threads"],
            full_refresh=dbt_cfg["full_refresh"],
        )

    # ---------------------------
    # Orquestación del pipeline
    # ---------------------------
    start = reset_logger()

    # Cadena principal
    start >> clear_tasks >> params >> web_scraping >> preprocess >> gcs_tasks >> bq_tasks

    # DBT depende de BQ y del `prepare_dbt`
    bq_tasks >> dbt_cfg >> bronze >> silver >> gold


dag = etl_generali_oop()
