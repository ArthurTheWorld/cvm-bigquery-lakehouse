from datetime import date

import pytest

import cvm_files as cf
import fetch_cvm as fc
from fakes import FakeBucket, FakeSession, latin1_csv, make_zip

DADOS = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
HEADER = "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DT_COMPTC;VL_QUOTA"


def month_zip(yyyymm, rows):
    return make_zip({f"inf_diario_fi_{yyyymm}.csv": latin1_csv(HEADER, rows)})


@pytest.fixture
def session():
    return FakeSession({
        f"{DADOS}/inf_diario_fi_202609.zip": month_zip("202609", ["FI;00.000.000/0001-91;2026-09-30;1.5"]),
        f"{DADOS}/HIST/inf_diario_fi_202508.zip": month_zip("202508", ["FI;00.000.000/0001-91;2025-08-29;1.4"]),
        cf.CAD_FI_URL: latin1_csv("CNPJ_FUNDO;DENOM_SOCIAL", ["00.000.000/0001-91;GESTÃO"]),
    })


def test_keys_follow_the_documented_layout():
    assert fc.inf_diario_key("202401") == "inf_diario/ano_mes=202401/inf_diario_fi_202401.csv"
    assert fc.cad_fi_key(date(2026, 10, 12)) == "cad_fi/data=20261012/cad_fi.csv"
    with pytest.raises(ValueError):
        fc.inf_diario_key("2024-01")


def test_fetch_writes_original_bytes_unchanged_and_uses_hist_fallback(tmp_path, session):
    summary = fc.fetch(["202508", "202609"], fc.LocalSink(tmp_path), session, today=date(2026, 10, 12), log=lambda _: None)
    assert summary == {"inf_diario": {"202508": "written", "202609": "written"}, "cad_fi": {"20261012": "written"}}
    written = (tmp_path / "inf_diario/ano_mes=202609/inf_diario_fi_202609.csv").read_bytes()
    assert written == latin1_csv(HEADER, ["FI;00.000.000/0001-91;2026-09-30;1.5"])  # Latin-1, CRLF e ';' preservados
    assert "GESTÃO".encode("latin-1") in (tmp_path / "cad_fi/data=20261012/cad_fi.csv").read_bytes()


def test_second_run_skips_unchanged_files_but_rewrites_a_grown_month(tmp_path, session):
    sink = fc.LocalSink(tmp_path)
    fc.fetch(["202609"], sink, session, today=date(2026, 10, 12), log=lambda _: None)
    again = fc.fetch(["202609"], sink, session, today=date(2026, 10, 12), log=lambda _: None)
    assert again["inf_diario"]["202609"] == "skipped" and again["cad_fi"]["20261012"] == "skipped"

    # a CVM acrescentou um dia ao arquivo do mês corrente
    session.routes[f"{DADOS}/inf_diario_fi_202609.zip"] = month_zip(
        "202609", ["FI;00.000.000/0001-91;2026-09-30;1.5", "FI;00.000.000/0001-91;2026-10-01;1.6"])
    grown = fc.fetch(["202609"], sink, session, today=date(2026, 10, 13), log=lambda _: None)
    assert grown["inf_diario"]["202609"] == "written"
    assert grown["cad_fi"]["20261013"] == "written"  # snapshot novo, chave nova (data da execução)


def test_missing_month_fails_loudly_instead_of_silently_skipping(tmp_path, session):
    with pytest.raises(cf.DownloadError, match="nenhuma URL candidata"):
        fc.fetch(["202601"], fc.LocalSink(tmp_path), session, log=lambda _: None)


def test_skip_cadastro(tmp_path, session):
    summary = fc.fetch(["202609"], fc.LocalSink(tmp_path), session, include_cadastro=False, log=lambda _: None)
    assert summary["cad_fi"] == {} and not (tmp_path / "cad_fi").exists()


def test_gcs_sink_is_idempotent_by_md5():
    bucket = FakeBucket()
    sink = fc.GcsSink(bucket, "cvm")
    assert sink.put("a/b.csv", b"1") == "written"
    assert sink.put("a/b.csv", b"1") == "skipped"
    assert sink.put("a/b.csv", b"2") == "written"
    assert list(bucket.store) == ["cvm/a/b.csv"] and bucket.store["cvm/a/b.csv"] == b"2"


def test_local_sink_prefix(tmp_path):
    fc.LocalSink(tmp_path, "/cvm/").put("x.csv", b"1")
    assert (tmp_path / "cvm" / "x.csv").read_bytes() == b"1"


def test_html_maintenance_page_served_as_200_is_rejected_before_landing(tmp_path):
    html = b"<html><body>Portal em manutencao</body></html>"
    sink = fc.LocalSink(tmp_path)

    zipped = FakeSession({f"{DADOS}/inf_diario_fi_202609.zip": make_zip({"inf_diario_fi_202609.csv": html})})
    with pytest.raises(ValueError, match="não parece um CSV da CVM"):
        fc.fetch(["202609"], sink, zipped, include_cadastro=False, log=lambda _: None)

    cad = FakeSession({cf.CAD_FI_URL: html})
    with pytest.raises(ValueError, match="cad_fi.csv"):
        fc.fetch([], sink, cad, log=lambda _: None)
    assert not list(tmp_path.rglob("*.csv"))  # nada foi depositado


def test_old_schema_with_cnpj_fundo_is_also_accepted(tmp_path):
    old = make_zip({"inf_diario_fi_202001.csv": latin1_csv("TP_FUNDO;CNPJ_FUNDO;DT_COMPTC;VL_QUOTA", ["FI;00.000.000/0001-91;2020-01-02;1.0"])})
    session = FakeSession({f"{DADOS}/inf_diario_fi_202001.zip": old})
    assert fc.fetch(["202001"], fc.LocalSink(tmp_path), session, include_cadastro=False, log=lambda _: None)["inf_diario"] == {"202001": "written"}
