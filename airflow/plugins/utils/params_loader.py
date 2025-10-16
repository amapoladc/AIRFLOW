# /opt/airflow/plugins/utils/params_loader.py
from __future__ import annotations

import os
import re
import yaml
import copy
import datetime as dt
from typing import Any, Dict, List

# ---------------------------------------
# Helpers internos
# ---------------------------------------

# Detecta patrones tipo ${ENV:VAR|default}
_ENV_PATTERN = re.compile(r"\$\{ENV:([A-Z0-9_]+)(\|[^}]*)?\}", re.IGNORECASE)

def _substitute_env(value: str) -> str:
    """Reemplaza variables de entorno en cadenas con formato ${ENV:VAR|default}."""
    def repl(match: re.Match):
        var = match.group(1)
        default = (match.group(2) or "").lstrip("|")
        return os.getenv(var, default)
    return _ENV_PATTERN.sub(repl, value)

def _normalize_value(value: Any) -> Any:
    """
    Convierte YES/NO a booleanos, sustituye variables de entorno y normaliza valores.
    - Cadenas: sustituye ${ENV:...}, trim y convierte true/false/yes/no/1/0 a bool.
    - Listas/Dicts: aplica recursivamente.
    """
    if isinstance(value, str):
        value = _substitute_env(value).strip()
        low = value.lower()
        if low in {"true", "yes", "y", "1"}:
            return True
        if low in {"false", "no", "n", "0", ""}:
            return False
        return value
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalize_value(v) for k, v in value.items()}
    return value

def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Mezcla profunda de dos diccionarios."""
    result = copy.deepcopy(a)
    for k, v in b.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result

# ---------------------------------------
# Clase principal ParamReader
# ---------------------------------------

class ParamReader:
    """
    Lee parámetros desde un archivo YAML (en vez de Excel) y mantiene compatibilidad con tu DAG:
    - read_yaml(): retorna una lista de dicts (un dict por bloque)
    - by_prefix(): filtra por prefijo ('PCW', 'PM1', 'PCGCS', 'PCBQ', 'MAIL', 'DBT', etc.)
    """

    @staticmethod
    def read_yaml(
        project_path: str,
        defaults_path: str | None = None,
        execution_date: dt.date | None = None
    ) -> List[Dict[str, Any]]:
        """
        Carga el archivo YAML del proyecto, aplica defaults, normaliza y lo devuelve como lista de dicts.
        Además:
        - Deriva run_date a partir de PCW1.timeDelta si existe.
        - Añade 'run_date' y 'run_date_nodash' a nivel raíz (en 'data' final).
        - Convierte cada bloque dict en un item con clave auxiliar '_BLOCK' para su nombre.
        """
        # Carga defaults si existen
        base: Dict[str, Any] = {}
        if defaults_path and os.path.exists(defaults_path):
            with open(defaults_path, "r", encoding="utf-8") as f:
                base = yaml.safe_load(f) or {}

        # Carga YAML del proyecto
        with open(project_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        # Merge y normalización
        data = _deep_merge(base, data)
        data = _normalize_value(data)

        # Derivar run_date a partir de timeDelta (si está presente)
        td = 0
        try:
            td = int(data.get("PCW1", {}).get("timeDelta", 0))
        except Exception:
            td = 0

        exec_date = execution_date or dt.date.today()
        run_date = exec_date - dt.timedelta(days=td)
        data["run_date"] = run_date.strftime(data.get("common", {}).get("run_date_fmt", "%Y-%m-%d"))
        data["run_date_nodash"] = run_date.strftime("%Y%m%d")

        # Convierte cada bloque en un dict independiente (como Excel original)
        blocks: List[Dict[str, Any]] = []
        for key, val in data.items():
            if isinstance(val, dict):
                block = dict(val)
                block["_BLOCK"] = key
                blocks.append(block)

        return blocks

    @staticmethod
    def by_prefix(data: Any, prefix: str) -> Any:
        """
        Filtra parámetros por nombre de bloque (ej: 'PCW', 'PM1', 'PCGCS', 'PCBQ', 'DBT').
        Acepta 'PCW' y 'PCW1' indistintamente.

        Soporta dos casos:
        - data = List[Dict[str, Any]] (modo síncrono): filtra inmediatamente.
        - data = XComArg (salida de otra @task): crea una mini-tarea que filtra en runtime.

        Devolverá:
        - List[Dict[str, Any]] si 'data' era lista real.
        - XComArg si 'data' era XComArg (la mini-tarea produce la lista filtrada).
        """
        prefix_u = prefix.upper()

        # Intento de detección de clase XComArg según versión de Airflow
        XComArgCls = None
        try:
            # Airflow 2.8+ (hay sdk.definitions)
            from airflow.sdk.definitions.xcom_arg import XComArg as _XComArg  # type: ignore
            XComArgCls = _XComArg
        except Exception:
            try:
                # Airflow <=2.7
                from airflow.models.xcom_arg import XComArg as _XComArg  # type: ignore
                XComArgCls = _XComArg
            except Exception:
                XComArgCls = None

        # Si es XComArg, definimos una mini-task que hace el filtrado en runtime
        if XComArgCls and isinstance(data, XComArgCls):
            from airflow.decorators import task

            @task(task_id=f"filter_params_{prefix_u.lower()}")
            def _filter_runtime(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                return [i for i in items if str(i.get("_BLOCK", "")).upper().startswith(prefix_u)]

            return _filter_runtime(data)

        # Caso normal: 'data' es lista real
        if not isinstance(data, list):
            raise TypeError(
                f"ParamReader.by_prefix espera List[Dict[str, Any]] o XComArg; "
                f"recibido: {type(data).__name__}"
            )

        return [item for item in data if str(item.get("_BLOCK", "")).upper().startswith(prefix_u)]
