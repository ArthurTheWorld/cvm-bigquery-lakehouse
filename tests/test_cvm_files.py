from datetime import date

import pytest

import cvm_files as cf
from fakes import FakeSession, latin1_csv, make_zip

HEADER = "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;ID_SUBCLASSE;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST"


def test_months_back_includes_current_month_oldest_first():
    assert cf.months_back(date(2026, 10, 12), 3) == ["202608", "202609", "202610"]


def test_months_back_crosses_year_boundary():
    assert cf.months_back(date(2026, 2, 5), 4) == ["202511", "202512", "202601", "202602"]
    assert cf.months_back(date(2026, 1, 31), 1) == ["202601"]


def test_months_back_rejects_zero():
    with pytest.raises(ValueError):
        cf.months_back(date(2026, 1, 1), 0)


def test_month_range_and_validation():
    assert cf.month_range("202511", "202602") == ["202511", "202512", "202601", "202602"]
    with pytest.raises(ValueError):
        cf.month_range("202602", "202511")
    for bad in ["2026-01", "202613", "202600", "26013", "", None, "202601; rm -rf"]:
        with pytest.raises(ValueError):
            cf.validate_month(bad)


def test_candidates_recent_month_tries_dados_then_hist():
    urls = cf.inf_diario_candidates("202510")
    assert urls[0].endswith("/INF_DIARIO/DADOS/inf_diario_fi_202510.zip")
    assert urls[1].endswith("/INF_DIARIO/DADOS/HIST/inf_diario_fi_202510.zip")
    assert len(urls) == 2


def test_candidates_old_year_also_tries_yearly_hist_zip():
    assert cf.inf_diario_candidates("201903")[-1].endswith("/HIST/inf_diario_fi_2019.zip")


def test_download_distinguishes_404_from_server_error():
    session = FakeSession({"http://ok": b"data", "http://down": 503})
    assert cf.download("http://ok", session) == b"data"
    assert cf.download("http://missing", session) is None
    with pytest.raises(cf.DownloadError, match="503"):
        cf.download("http://down", session)


def test_fetch_first_falls_back_and_reports_all_tried_urls():
    session = FakeSession({"http://b": b"found"})
    assert cf.fetch_first(["http://a", "http://b", "http://c"], session) == ("http://b", b"found")
    assert session.calls == ["http://a", "http://b"]  # para no primeiro que existe
    with pytest.raises(cf.DownloadError, match="http://x.*http://y"):
        cf.fetch_first(["http://x", "http://y"], FakeSession({}))


def test_a_server_error_is_not_swallowed_by_fallback():
    with pytest.raises(cf.DownloadError, match="500"):
        cf.fetch_first(["http://a", "http://b"], FakeSession({"http://a": 500, "http://b": b"x"}))


def test_extract_and_pick_month_from_monthly_and_yearly_zips():
    monthly = cf.extract_csvs(make_zip({"inf_diario_fi_202401.csv": b"x", "leiame.txt": b"ignorar"}))
    assert list(monthly) == ["inf_diario_fi_202401.csv"]
    assert cf.pick_month_csv(monthly, "202401") == b"x"

    yearly = cf.extract_csvs(make_zip({f"inf_diario_fi_2019{m:02d}.csv": str(m).encode() for m in range(1, 13)}))
    assert cf.pick_month_csv(yearly, "201905") == b"5"
    with pytest.raises(cf.DownloadError):
        cf.pick_month_csv(yearly, "202001")


def test_header_and_values_survive_latin1_accents():
    data = latin1_csv("CNPJ_FUNDO;DENOM_SOCIAL", ["00.000.000/0001-91;FUNDO DE INVESTIMENTO EM AÇÕES GESTÃO"])
    assert cf.csv_header(data) == ["cnpj_fundo", "denom_social"]
    frame = cf.read_csv_bytes(data)
    assert frame.loc[0, "denom_social"] == "FUNDO DE INVESTIMENTO EM AÇÕES GESTÃO"


def test_read_csv_keeps_everything_as_text_and_empty_as_empty():
    frame = cf.read_csv_bytes(latin1_csv(HEADER, ["FI;00.000.000/0001-91;;2024-01-02;100.50;1.123456789012;99.00;;5.00;0007"]))
    row = frame.iloc[0]
    assert row["id_subclasse"] == "" and row["captc_dia"] == ""        # vazio continua vazio (não NaN)
    assert row["nr_cotst"] == "0007" and row["vl_quota"] == "1.123456789012"  # zeros e 12 casas intactos


@pytest.mark.parametrize("bad", ["", "A;;B", "A;A"])
def test_header_rejects_empty_or_duplicate_columns(bad):
    with pytest.raises(ValueError):
        cf.csv_header(bad.encode("latin-1"))


def test_header_may_come_in_any_order():
    assert cf.csv_header(b"SIT;CNPJ_FUNDO;ADMIN\r\n") == ["sit", "cnpj_fundo", "admin"]


def test_normalize_cnpj():
    assert cf.normalize_cnpj("00.000.000/0001-91") == "00000000000191"
    assert cf.normalize_cnpj("") == "" and cf.normalize_cnpj(None) == ""
