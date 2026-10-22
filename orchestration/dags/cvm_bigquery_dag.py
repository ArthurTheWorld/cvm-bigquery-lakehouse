"""
DAG: CVM (informe diário + cadastro) -> GCS -> BigQuery raw -> dbt (staging/marts).

  fetch_to_gcs -> load_raw -> dbt_source_freshness -> dbt_build

Dias úteis, 08h de Brasília. Idempotente de ponta a ponta: o download pula arquivos
idênticos, a carga substitui a partição do mês e o dbt reprocessa por _loaded_at.

Cloud Composer custa caro para um projeto de portfólio (o ambiente fica ligado o mês
inteiro): suba o ambiente só para demonstrar e destrua depois (ver README).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cvm_tasks  # noqa: E402


def _context_date(context) -> datetime:
    value = context.get("logical_date") or context.get("data_interval_end") or datetime.utcnow()
    return value


def _fetch(**context):
    cfg = cvm_tasks.settings()
    run_date = _context_date(context).date()
    months = cvm_tasks.months_for_run(run_date, cfg["months_back"])
    summary = cvm_tasks.run_fetch(months, cfg["bucket"], cfg["prefix"], today=run_date)
    return {"months": months, "cad_date": f"{run_date:%Y%m%d}", "summary": summary}


def _load(ti, **context):
    cfg = cvm_tasks.settings()
    fetched = ti.xcom_pull(task_ids="fetch_to_gcs")
    return cvm_tasks.run_load(fetched["months"], fetched["cad_date"], cfg["project"], cfg["bucket"], cfg["prefix"])


DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/home/airflow/gcs/dags/dbt_project")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")  # dev = OAuth/ADC; no Composer é a conta de serviço do ambiente

with DAG(
    dag_id="cvm_bigquery_medallion",
    description="Baixa a CVM, carrega no BigQuery raw e roda o dbt (staging/marts).",
    start_date=datetime(2026, 1, 1),
    schedule="0 11 * * 1-5",  # 11h UTC = 08h em Brasília, dias úteis
    catchup=False,
    max_active_runs=1,  # nunca dois dbt em paralelo sobre as mesmas tabelas
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["gcp", "bigquery", "dbt", "cvm"],
) as dag:
    fetch_to_gcs = PythonOperator(task_id="fetch_to_gcs", python_callable=_fetch)
    load_raw = PythonOperator(task_id="load_raw", python_callable=_load)
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --target {DBT_TARGET}",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --target {DBT_TARGET}",
    )
    fetch_to_gcs >> load_raw >> source_freshness >> dbt_build
