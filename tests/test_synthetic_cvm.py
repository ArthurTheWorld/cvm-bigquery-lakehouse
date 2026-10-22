import json

import numpy as np
import pandas as pd
import pytest

import cvm_files as cf
import synthetic_cvm as sc


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    out = tmp_path_factory.mktemp("syn")
    truth = sc.generate(out, funds=160, months=4, start_month="202501", seed=7, schema_cutover="202503")
    files = {p.parent.name.split("=")[1]: p for p in sorted(out.glob("inf_diario/*/*.csv"))}
    frames = {m: cf.read_csv_bytes(p.read_bytes()) for m, p in files.items()}
    cad = cf.read_csv_bytes(next(out.glob("cad_fi/*/cad_fi.csv")).read_bytes())
    return out, truth, frames, cad


def unified(frames):
    """Todos os meses num só DataFrame com nomes novos de coluna, para conferir contagens."""
    parts = []
    for month, f in frames.items():
        f = f.rename(columns={"tp_fundo": "tp_fundo_classe", "cnpj_fundo": "cnpj_fundo_classe"})
        if "id_subclasse" not in f:
            f["id_subclasse"] = ""
        parts.append(f.assign(month=month))
    return pd.concat(parts, ignore_index=True)


# ------------------------------------------------------------ documentos
def test_known_valid_documents():
    assert sc.valid_cnpj("11.222.333/0001-81")
    assert not sc.valid_cnpj("11.222.333/0001-82")
    assert sc.cpf_from_base("529982247") == "529.982.247-25"


def test_generated_cnpjs_are_valid_and_unique(data):
    _, _, frames, cad = data
    informe = unified(frames)
    assert informe["cnpj_fundo_classe"].map(sc.valid_cnpj).all()
    assert cad["cnpj_fundo"].map(sc.valid_cnpj).all() and cad["cnpj_fundo"].is_unique


# ------------------------------------------------------------- determinismo
def test_same_seed_same_bytes_and_other_seed_differs(tmp_path):
    a, b, c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    sc.generate(a, funds=40, months=2, seed=3)
    sc.generate(b, funds=40, months=2, seed=3)
    sc.generate(c, funds=40, months=2, seed=4)
    rel = "inf_diario/ano_mes=202501/inf_diario_fi_202501.csv"
    assert (a / rel).read_bytes() == (b / rel).read_bytes() != (c / rel).read_bytes()


# ------------------------------------------------------- formato do arquivo
def test_layout_and_encoding(data):
    out, truth, frames, _ = data
    assert sorted(frames) == truth["months"]
    raw = next(out.glob("inf_diario/*/*.csv")).read_bytes()
    assert b"\r\n" in raw and b";" in raw
    assert "ÁGUIA".encode("latin-1") in next(out.glob("cad_fi/*/cad_fi.csv")).read_bytes()  # acentos em Latin-1, não UTF-8


def test_schema_changes_at_the_cutover(data):
    _, _, frames, _ = data
    assert list(frames["202501"].columns) == [c.lower() for c in sc.INF_OLD]
    assert list(frames["202502"].columns) == [c.lower() for c in sc.INF_OLD]
    assert list(frames["202503"].columns) == [c.lower() for c in sc.INF_NEW]
    assert list(frames["202504"].columns) == [c.lower() for c in sc.INF_NEW]


def test_every_informe_header_passes_the_fetch_validation(data):
    _, _, frames, _ = data
    for frame in frames.values():
        cf.require_columns(list(frame.columns), cf.INF_DIARIO_REQUIRED, "informe")


def test_cadastro_has_the_41_official_fields_in_a_non_alphabetical_order(data):
    _, _, _, cad = data
    assert len(cad.columns) == 41
    assert sorted(c.upper() for c in cad.columns) == sorted(sc.CAD_FI_COLUMNS)
    assert list(cad.columns) != sorted(cad.columns)  # ordem do arquivo != ordem do dicionário
    cf.require_columns(list(cad.columns), cf.CAD_FI_REQUIRED, "cad_fi")


def test_quota_has_12_decimals_and_dates_are_iso(data):
    _, _, frames, _ = data
    f = frames["202504"]
    assert f["vl_quota"].str.match(r"^-?\d+\.\d{12}$").all()
    assert f["dt_comptc"].str.match(r"^\d{4}-\d{2}-\d{2}$").all()
    assert (f["dt_comptc"].str[:7] == "2025-04").all()  # cada arquivo só tem dias do seu mês


def test_optional_fields_come_blank_not_nan(data):
    _, _, frames, _ = data
    assert (unified(frames)["captc_dia"] == "").any()


# ---------------------------------------------- a "verdade" bate com os arquivos
def test_truth_row_counts(data):
    _, truth, frames, _ = data
    informe = unified(frames)
    assert len(informe) == truth["informe_rows_in_files"]
    keys = ["cnpj_fundo_classe", "id_subclasse", "dt_comptc"]
    assert len(informe.drop_duplicates(keys, keep="last")) == truth["informe_rows_after_dedupe"]
    assert len(informe) - len(informe.drop_duplicates(keys)) == truth["duplicate_rows"] > 0


def test_corrections_come_later_in_the_file_than_the_original(data):
    _, truth, frames, _ = data
    informe = unified(frames)
    for c in truth["corrected"][:10]:
        rows = informe[(informe["cnpj_fundo_classe"] == c["cnpj"]) & (informe["id_subclasse"] == c["id_subclasse"])
                       & (informe["dt_comptc"] == c["dt_comptc"])]
        assert len(rows) == 2 and rows.iloc[-1]["vl_quota"] == c["vl_quota"] != rows.iloc[0]["vl_quota"]


def test_truth_defect_counts(data):
    _, truth, frames, _ = data
    informe = unified(frames).drop_duplicates(["cnpj_fundo_classe", "id_subclasse", "dt_comptc"], keep="last")
    assert (informe["vl_quota"].astype(float) <= 0).sum() == truth["nonpositive_quota_rows"] > 0
    assert (informe["vl_patrim_liq"].astype(float) < 0).sum() == truth["negative_pl_rows"] > 0


def test_orphans_and_cadastro_only_funds(data):
    _, truth, frames, cad = data
    informe_cnpjs = set(unified(frames)["cnpj_fundo_classe"])
    assert len(informe_cnpjs - set(cad["cnpj_fundo"])) == truth["orphan_funds_in_informe"] > 0
    assert truth["funds_in_cadastro"] == len(cad) and truth["funds_in_informe"] == len(informe_cnpjs)


def test_each_spike_creates_exactly_two_daily_returns_beyond_50_percent(data):
    _, truth, frames, _ = data
    d = unified(frames).drop_duplicates(["cnpj_fundo_classe", "id_subclasse", "dt_comptc"], keep="last")
    d["q"] = d["vl_quota"].astype(float)
    d = d.sort_values(["cnpj_fundo_classe", "id_subclasse", "dt_comptc"])
    d["prev"] = d.groupby(["cnpj_fundo_classe", "id_subclasse"])["q"].shift()
    valid = d[(d["q"] > 0) & (d["prev"] > 0)]
    outliers = (valid["q"] / valid["prev"] - 1).abs() > 0.5
    assert outliers.sum() == 2 * truth["spike_funds"] > 0


def test_defects_are_in_disjoint_funds(data):
    """Se um fundo tivesse dois defeitos, as contagens esperadas deixariam de ser exatas."""
    _, truth, frames, _ = data
    d = unified(frames).drop_duplicates(["cnpj_fundo_classe", "id_subclasse", "dt_comptc"], keep="last")
    bad_q = set(d.loc[d["vl_quota"].astype(float) <= 0, "cnpj_fundo_classe"])
    bad_pl = set(d.loc[d["vl_patrim_liq"].astype(float) < 0, "cnpj_fundo_classe"])
    assert not bad_q & bad_pl
