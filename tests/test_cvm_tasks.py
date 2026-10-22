from datetime import date

import pytest

import cvm_files as cf
import cvm_tasks as ct
import fetch_cvm as fc
from fakes import FakeBucket, FakeSession, latin1_csv, make_zip
from test_load_bigquery import COLS, FakeBQ, FakeReader, INF_NEW, ROW, loader

DADOS = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"


class FakeStorage:
    def __init__(self):
        self.bucket_obj = FakeBucket()

    def bucket(self, name):
        self.last = name
        return self.bucket_obj


def test_months_for_run_covers_current_and_previous_month():
    assert ct.months_for_run(date(2026, 10, 12), 2) == ["202609", "202610"]
    assert ct.months_for_run(date(2026, 1, 5), 2) == ["202512", "202601"]


def test_run_fetch_lands_files_in_the_bucket_under_the_prefix():
    storage = FakeStorage()
    session = FakeSession({
        f"{DADOS}/inf_diario_fi_202610.zip": make_zip({"inf_diario_fi_202610.csv": latin1_csv(INF_NEW, [ROW])}),
        cf.CAD_FI_URL: latin1_csv("CNPJ_FUNDO;DENOM_SOCIAL", ["00.000.000/0001-91;X"]),
    })
    summary = ct.run_fetch(["202610"], "meu-bucket", "cvm", today=date(2026, 10, 12), storage_client=storage, session=session)
    assert storage.last == "meu-bucket"
    assert sorted(storage.bucket_obj.store) == ["cvm/cad_fi/data=20261012/cad_fi.csv", "cvm/inf_diario/ano_mes=202610/inf_diario_fi_202610.csv"]
    assert summary["inf_diario"] == {"202610": "written"}


def test_run_load_loads_every_month_of_the_window_even_if_the_download_skipped_them():
    """Se o download gravou e a carga falhou, a execução seguinte vê o arquivo como 'igual' (skipped):
    a carga NÃO pode depender disso."""
    uri = lambda m: f"gs://b/cvm/{fc.inf_diario_key(m)}"
    cad_uri = f"gs://b/cvm/{fc.cad_fi_key(date(2026, 10, 12))}"
    files = {uri("202609"): latin1_csv(INF_NEW, [ROW]), uri("202610"): latin1_csv(INF_NEW, [ROW]),
             cad_uri: latin1_csv("CNPJ_FUNDO;DENOM_SOCIAL", ["00.000.000/0001-91;X"])}
    bq = FakeBQ(existing=COLS + ["cnpj_fundo", "denom_social"])
    result = ct.run_load(["202609", "202610"], "20261012", "meu-projeto-gcp", "b", "cvm", loader=loader(bq, files))
    assert sorted(result["inf_diario"]) == ["202609", "202610"] and list(result["cad_fi"]) == ["20261012"]
    assert sum(1 for s in bq.sqls() if s.startswith("BEGIN TRANSACTION")) == 3  # 2 meses + 1 snapshot


def test_settings_requires_project_and_bucket(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.delenv("CVM_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="GCP_PROJECT"):
        ct.settings()
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.setenv("CVM_BUCKET", "b")
    cfg = ct.settings()
    assert cfg["prefix"] == "cvm" and cfg["months_back"] == 2 and cfg["dbt_target"] == "dev"
