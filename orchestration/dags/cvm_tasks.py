"""Lógica das tarefas da DAG, sem dependência de Airflow nem de bibliotecas do Google.

As dependências (clientes do GCS e do BigQuery, sessão HTTP) são injetáveis, então os
testes rodam só com dublês.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

# Layout do repositório (../../ingestion) ou módulos copiados para a pasta da DAG.
for candidate in (Path(__file__).resolve().parent, Path(__file__).resolve().parents[2] / "ingestion"):
    if (candidate / "fetch_cvm.py").exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import cvm_files as cf  # noqa: E402
import fetch_cvm  # noqa: E402
import load_bigquery as lb  # noqa: E402


def months_for_run(run_date: date, months_back: int = 2) -> List[str]:
    """O mês corrente e os anteriores. O corrente muda todo dia útil e a CVM aceita
    reapresentações do mês anterior, então os dois são sempre reprocessados."""
    return cf.months_back(run_date, months_back)


def run_fetch(months: List[str], bucket_name: str, prefix: str, today: Optional[date] = None,
              storage_client=None, session=None) -> Dict[str, Dict[str, str]]:
    if storage_client is None:
        from google.cloud import storage

        storage_client = storage.Client()
    if session is None:
        import requests

        session = requests.Session()
        session.headers["User-Agent"] = "cvm-bigquery-lakehouse (portfolio; dados abertos)"
    sink = fetch_cvm.GcsSink(storage_client.bucket(bucket_name), prefix)
    return fetch_cvm.fetch(months, sink, session, today=today)


def run_load(months: List[str], cad_date: str, project: str, bucket: str, prefix: str,
             dataset: str = "raw", loader: Optional[lb.RawLoader] = None) -> Dict[str, Dict]:
    """Carrega TODOS os meses da janela, não só os que o download acabou de gravar.

    Se o download gravar e a carga falhar, a execução seguinte veria o arquivo como "igual"
    e nunca o carregaria. A carga é idempotente por partição, então repetir é seguro."""
    if loader is None:
        from google.cloud import bigquery

        loader = lb.RawLoader(bigquery.Client(project=project), project, dataset)
    base = f"gs://{bucket}/{prefix.strip('/')}"
    result: Dict[str, Dict] = {"inf_diario": {}, "cad_fi": {}}
    for month in months:
        result["inf_diario"][month] = loader.load_inf_diario(f"{base}/{fetch_cvm.inf_diario_key(month)}", month)
    snapshot = datetime.strptime(cad_date, "%Y%m%d").date()
    result["cad_fi"][cad_date] = loader.load_cad_fi(f"{base}/{fetch_cvm.cad_fi_key(snapshot)}", cad_date)
    return result


def settings() -> Dict[str, str]:
    """Configuração por variável de ambiente (no Composer: variáveis de ambiente do ambiente)."""
    required = ["GCP_PROJECT", "CVM_BUCKET"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"variáveis de ambiente ausentes: {missing}")
    return {
        "project": os.environ["GCP_PROJECT"],
        "bucket": os.environ["CVM_BUCKET"],
        "prefix": os.getenv("CVM_PREFIX", "cvm"),
        "months_back": int(os.getenv("CVM_MONTHS_BACK", "2")),
        "dbt_dir": os.getenv("DBT_PROJECT_DIR", "/home/airflow/gcs/dags/dbt_project"),
        "dbt_target": os.getenv("DBT_TARGET", "dev"),
    }
