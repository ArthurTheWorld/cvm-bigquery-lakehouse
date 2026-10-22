"""Derruba os datasets EFÊMEROS que o CI do dbt cria no BigQuery.

O CI roda o dbt com `schema_prefix=dbt_ci_<run_id>_`, então escreve em
`dbt_ci_<run_id>_staging` e `dbt_ci_<run_id>_marts`, sem tocar nos datasets reais. Este script
os apaga no fim.

Trava de segurança: só aceita um prefixo no formato `dbt_ci_<alfanumérico>_`. Um erro de
variável (prefixo vazio, "raw", "marts") não consegue apagar um dataset de verdade.

Uso:  python ci/bq_cleanup.py dbt_ci_123456_
Env:  GCP_PROJECT (e credenciais do Google)
"""
from __future__ import annotations

import os
import re
import sys
from typing import List

LAYERS = ("staging", "marts")
PREFIX = re.compile(r"^dbt_ci_[A-Za-z0-9]+_$")


def datasets_to_drop(prefix: str) -> List[str]:
    if not PREFIX.match(prefix or ""):
        raise ValueError(f"prefixo recusado: {prefix!r} (esperado dbt_ci_<id>_)")
    return [f"{prefix}{layer}" for layer in LAYERS]


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    datasets = datasets_to_drop(argv[1])
    from google.cloud import bigquery  # import tardio: os testes não precisam da biblioteca

    client = bigquery.Client(project=os.environ["GCP_PROJECT"])
    for dataset in datasets:
        print(f"> drop dataset {dataset}")
        client.delete_dataset(f"{client.project}.{dataset}", delete_contents=True, not_found_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
