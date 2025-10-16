# /opt/airflow/plugins/operators/dbt_operator.py
from __future__ import annotations
import json
import os
import shlex
import shutil
import signal
import subprocess
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.utils.context import Context


class DbtOperator(BaseOperator):
    """
    Run dbt CLI (run/test/seed/snapshot/build/compile/docs generate) in the worker.
    """

    template_fields: Sequence[str] = (
        "project_dir",
        "profiles_dir",
        "select",
        "exclude",
        "vars",
        "target",
        "state",
        "env",           # <- existe self.env
        "threads",
        "full_refresh",
    )
    template_fields_renderers = {
        "vars": "json",
        "env": "json",
    }
    ui_color = "#52489C"

    def __init__(
        self,
        *,
        command: str = "run",
        project_dir: str,
        profiles_dir: str,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[str] = None,
        vars: Optional[Dict[str, Any]] = None,
        target: Optional[str] = None,
        full_refresh: bool | str = False,
        state: Optional[str] = None,
        fail_fast: bool = False,
        threads: Optional[int | str] = None,
        env: Optional[Dict[str, str] | str] = None,   # templated
        xcom_push_artifacts: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.select = select
        self.exclude = exclude
        self.vars = vars if vars is not None else {}
        self.target = target
        self.full_refresh = full_refresh
        self.state = state
        self.fail_fast = fail_fast
        self.threads = threads

        # atributo env real + alias extra_env
        self.env = env if env is not None else {}
        self.extra_env = self.env

        self.xcom_push_artifacts = xcom_push_artifacts
        self._proc: Optional[subprocess.Popen] = None

    # ---------- helpers ----------
    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        s = str(value).strip().lower()
        return s in {"1", "true", "yes", "y", "t"}

    @staticmethod
    def _maybe_json_load(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return value
        return value

    def _normalize_templated_types(self) -> None:
        # vars/env pueden llegar como string JSON desde Jinja
        self.vars = self._maybe_json_load(self.vars)
        if not isinstance(self.vars, dict):
            self.vars = {}

        self.env = self._maybe_json_load(self.env)
        if not isinstance(self.env, dict):
            self.env = {}
        # alias
        self.extra_env = self.env

        # threads puede llegar como str
        if isinstance(self.threads, str):
            st = self.threads.strip()
            self.threads = int(st) if st.isdigit() else None

        # full_refresh puede llegar como str/int
        if isinstance(self.full_refresh, (str, int)):
            self.full_refresh = self._to_bool(self.full_refresh)

        # select: admite str (con espacios/commas) o list[str]
        if isinstance(self.select, str):
            parts = []
            for token in self.select.replace(",", " ").split():
                tok = token.strip()
                if tok:
                    parts.append(tok)
            self.select = parts if len(parts) > 1 else (parts[0] if parts else None)
        elif isinstance(self.select, list):
            self.select = [str(s).strip() for s in self.select if str(s).strip()]
            if not self.select:
                self.select = None

    def _build_cmd(self) -> List[str]:
        base = ["dbt"] + shlex.split(self.command)
        base += ["--project-dir", self.project_dir]
        base += ["--profiles-dir", self.profiles_dir]

        if self.select:
            if isinstance(self.select, list):
                for s in self.select:
                    base += ["--select", s]
            else:
                base += ["--select", self.select]

        if self.exclude:
            base += ["--exclude", self.exclude]
        if self.vars:
            base += ["--vars", json.dumps(self.vars)]
        if self.target:
            base += ["--target", self.target]
        if self.full_refresh:
            base += ["--full-refresh"]
        if self.state:
            base += ["--state", self.state]
        if self.fail_fast:
            base += ["--fail-fast"]
        if self.threads:
            base += ["--threads", str(self.threads)]
        return base

    def execute(self, context: Context) -> Optional[str]:
        self._normalize_templated_types()

        if not shutil.which("dbt"):
            raise RuntimeError(
                "dbt executable not found in PATH. Install dbt in the Airflow image "
                "(e.g. `pip install dbt-bigquery`) and rebuild."
            )
        if not os.path.isdir(self.project_dir):
            raise RuntimeError(f"DBT project directory not found: {self.project_dir}")
        if not os.path.isdir(self.profiles_dir):
            self.log.warning("Profiles dir %s does not exist; creating it.", self.profiles_dir)
            os.makedirs(self.profiles_dir, exist_ok=True)

        cmd = self._build_cmd()
        self.log.info("Executing: %s", " ".join(shlex.quote(c) for c in cmd))

        env = os.environ.copy()
        env.update(self.env)   # usar self.env (templated)

        self._proc = subprocess.Popen(
            cmd,
            cwd=self.project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,
        )

        lines: List[str] = []
        assert self._proc.stdout is not None
        for line in self._proc.stdout:
            self.log.info(line.rstrip())
            if self.xcom_push_artifacts:
                lines.append(line)

        code = self._proc.wait()
        if code != 0:
            raise RuntimeError(f"dbt command failed with exit code {code}")
        return "".join(lines) if self.xcom_push_artifacts else None

    def on_kill(self) -> None:
        if self._proc and self._proc.poll() is None:
            self.log.warning("Terminating dbt process (pid=%s)...", self._proc.pid)
            try:
                self._proc.terminate()
                self._proc.wait(timeout=10)
            except Exception:
                self.log.warning("Force killing dbt process...")
                try:
                    os.kill(self._proc.pid, signal.SIGKILL)
                except Exception:
                    pass
