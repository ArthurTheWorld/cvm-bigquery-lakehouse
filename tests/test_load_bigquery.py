import csv
import io
from datetime import date

import pytest

import load_bigquery as lb
from fakes import latin1_csv

INF_NEW = "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;ID_SUBCLASSE;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST"
ROW = "FI;00.000.000/0001-91;;2025-01-02;10.00;1.123456789012;9.00;;2.00;5"
URI = "gs://bucket/cvm/inf_diario/ano_mes=202501/inf_diario_fi_202501.csv"


class Job:
    def __init__(self, rows=None, error=None):
        self.rows, self.error = rows or [], error

    def result(self):
        if self.error:
            raise self.error
        return self.rows


class Column:
    def __init__(self, name):
        self.name = name


class FakeBQ:
    def __init__(self, existing=None, fail_load=None, count_override=None):
        self.calls, self.existing = [], existing
        self.fail_load, self.count_override = fail_load, count_override
        self.payloads = {}

    def load_table_from_file(self, fileobj, dest, job_config=None):
        self.calls.append(("load", dest, job_config))
        self.payloads[dest] = fileobj.read()
        return Job(error=self.fail_load)

    def query(self, sql):
        self.calls.append(("query", sql))
        if sql.startswith("SELECT COUNT(*)"):
            n = self.count_override
            if n is None:
                dest = next(d for c, d, *_ in self.calls if c == "load")
                n = max(0, self.payloads[dest].count(b"\n") - 1)
            return Job(rows=[(n,)])
        return Job()

    def get_table(self, ref):
        self.calls.append(("get_table", ref))
        return type("T", (), {"schema": [Column(c) for c in (self.existing or [])]})()

    def delete_table(self, ref, not_found_ok=False):
        self.calls.append(("delete", ref, not_found_ok))

    def kinds(self):
        return [c[0] for c in self.calls]

    def sqls(self):
        return [c[1] for c in self.calls if c[0] == "query"]


class FakeReader:
    def __init__(self, files):
        self.files = files

    def read(self, uri):
        return self.files[uri]


def loader(client, files, **kw):
    return lb.RawLoader(client, "meu-projeto-gcp", reader=FakeReader(files), config_factory=lambda cols: {"cols": cols},
                        run_id="r1", **kw)


FILE = latin1_csv(INF_NEW, [ROW, ROW.replace("2025-01-02", "2025-01-03")])
COLS = [c.lower() for c in INF_NEW.split(";")]


# ----------------------------------------------------------- normalize_csv
def test_normalize_converts_latin1_to_utf8_and_adds_row_numbers():
    data = latin1_csv("CNPJ_FUNDO;DENOM_SOCIAL", ["1;GESTÃO A", "2;AÇÕES B"])
    header, payload, n = lb.normalize_csv(data)
    assert header == ["cnpj_fundo", "denom_social"] and n == 2
    rows = list(csv.reader(io.StringIO(payload.decode("utf-8"))))
    assert rows == [["_src_row", "cnpj_fundo", "denom_social"], ["1", "1", "GESTÃO A"], ["2", "2", "AÇÕES B"]]


def test_normalize_keeps_quoted_separators_and_empty_fields():
    data = 'A;B;C\r\n"x;y";;z\r\n'.encode("latin-1")
    _, payload, _ = lb.normalize_csv(data)
    assert list(csv.reader(io.StringIO(payload.decode())))[1] == ["1", "x;y", "", "z"]


def test_normalize_ignores_blank_lines_and_a_missing_final_newline():
    assert lb.normalize_csv(b"A;B\r\n1;2\r\n\r\n")[2] == 1
    assert lb.normalize_csv(b"A;B\r\n1;2")[2] == 1


def test_normalize_fails_on_a_shifted_row_instead_of_silently_misaligning():
    with pytest.raises(ValueError, match="linha 3: 3 campos"):
        lb.normalize_csv(b"A;B\r\n1;2\r\n1;2;3\r\n")


@pytest.mark.parametrize("header", ["A;A", "A;`x`;B", "A; DROP TABLE x", "1A;B", "A-B;C", ""])
def test_normalize_rejects_unsafe_or_invalid_headers(header):
    with pytest.raises(ValueError):
        lb.normalize_csv((header + "\r\n1;2\r\n").encode("latin-1"))


# ---------------------------------------------------------------- SQL
def test_create_inf_diario_is_partitioned_by_month_and_uses_the_reserved_prefix_safe_names():
    sql = lb.create_table_sql("`p.raw.inf_diario`", ["a", "b"], "inf_diario")
    assert "PARTITION BY DATE_TRUNC(`_src_month`, MONTH)" in sql
    assert "`a` STRING" in sql and "`_src_row` INT64" in sql and "`_loaded_at` TIMESTAMP" in sql
    for reserved in ("_FILE_", "_TABLE_", "_PARTITION"):  # prefixos reservados pelo BigQuery
        assert not any(col.strip("` ").upper().startswith(reserved) for col in
                       [l.split()[0] for l in sql.splitlines() if l.startswith("  `")]), reserved


def test_create_cad_fi_is_daily_snapshot_with_expiration():
    sql = lb.create_table_sql("`p.raw.cad_fi`", ["a"], "cad_fi", 90)
    assert "PARTITION BY `_src_snapshot`" in sql and "partition_expiration_days = 90" in sql
    with pytest.raises(ValueError):
        lb.create_table_sql("`x`", ["a"], "outro")


def test_replace_script_is_a_transaction_with_delete_then_insert():
    s = lb.replace_partition_script("`p.raw.t`", "`p.raw.tmp`", ["a", "b"], "inf_diario", date(2025, 1, 1), "gs://b/k's.csv")
    assert s.startswith("BEGIN TRANSACTION;") and s.endswith("COMMIT TRANSACTION;")
    assert s.index("DELETE FROM") < s.index("INSERT INTO")
    assert "WHERE `_src_month` = DATE '2025-01-01'" in s
    assert "(`a`, `b`, `_src_row`, `_src_month`, `_src_file`, `_loaded_at`)" in s
    assert "'gs://b/k\\'s.csv'" in s  # aspas no caminho não quebram o SQL


# ----------------------------------------------------------------- fluxo
def test_first_load_creates_table_loads_and_cleans_up_in_order():
    bq = FakeBQ(existing=COLS + ["_src_row", "_src_month", "_src_file", "_loaded_at"])
    result = loader(bq, {URI: FILE}).load_inf_diario(URI, "202501")
    assert result == {"rows": 2, "columns": 10}
    assert bq.kinds() == ["load", "query", "get_table", "query", "query", "delete"]
    sqls = bq.sqls()
    assert sqls[0].startswith("CREATE TABLE IF NOT EXISTS `meu-projeto-gcp.raw.inf_diario`")
    assert "DELETE FROM" in sqls[1] and "DATE '2025-01-01'" in sqls[1]
    assert sqls[2].startswith("SELECT COUNT(*)")
    assert bq.calls[-1] == ("delete", "meu-projeto-gcp.raw._tmp_inf_diario_r1", True)


def test_load_job_uses_the_file_columns_and_loads_the_normalized_payload():
    bq = FakeBQ(existing=COLS)
    loader(bq, {URI: FILE}).load_inf_diario(URI, "202501")
    _, dest, config = bq.calls[0]
    assert dest == "meu-projeto-gcp.raw._tmp_inf_diario_r1" and config == {"cols": COLS}
    assert bq.payloads[dest].decode().splitlines()[0] == "_src_row," + ",".join(COLS)


def test_new_column_in_the_file_is_added_before_the_load_script():
    bq = FakeBQ(existing=[c for c in COLS if c != "id_subclasse"])  # tabela antiga, sem a coluna
    loader(bq, {URI: FILE}).load_inf_diario(URI, "202501")
    sqls = bq.sqls()
    alters = [s for s in sqls if s.startswith("ALTER TABLE")]
    assert alters == ["ALTER TABLE `meu-projeto-gcp.raw.inf_diario` ADD COLUMN IF NOT EXISTS `id_subclasse` STRING"]
    assert sqls.index(alters[0]) < next(i for i, s in enumerate(sqls) if s.startswith("BEGIN TRANSACTION"))


def test_column_missing_from_the_file_is_left_out_of_the_insert_so_it_stays_null():
    old_file = latin1_csv("TP_FUNDO;CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA", ["FI;00.000.000/0001-91;2025-01-02;1;1"])
    bq = FakeBQ(existing=COLS + ["tp_fundo", "cnpj_fundo"])  # a tabela já conhece o schema novo E o antigo
    loader(bq, {URI: old_file}).load_inf_diario(URI, "202501")
    script = next(s for s in bq.sqls() if s.startswith("BEGIN TRANSACTION"))
    assert "`cnpj_fundo`" in script and "`cnpj_fundo_classe`" not in script and "`id_subclasse`" not in script
    assert not [s for s in bq.sqls() if s.startswith("ALTER TABLE")]


def test_reloading_the_same_month_issues_the_same_idempotent_statements():
    first, second = FakeBQ(existing=COLS), FakeBQ(existing=COLS)
    loader(first, {URI: FILE}).load_inf_diario(URI, "202501")
    loader(second, {URI: FILE}).load_inf_diario(URI, "202501")
    assert first.sqls() == second.sqls()
    assert "DELETE FROM" in first.sqls()[1]  # apaga a partição antes de inserir: sem duplicar


def test_failed_load_job_drops_the_temp_table_and_runs_no_dml():
    bq = FakeBQ(fail_load=RuntimeError("quota"))
    with pytest.raises(RuntimeError, match="quota"):
        loader(bq, {URI: FILE}).load_inf_diario(URI, "202501")
    assert bq.kinds() == ["load", "delete"] and not bq.sqls()


def test_count_mismatch_after_load_is_an_error_and_still_cleans_up():
    bq = FakeBQ(existing=COLS, count_override=1)
    with pytest.raises(lb.LoadError, match="2 linhas, a partição ficou com 1"):
        loader(bq, {URI: FILE}).load_inf_diario(URI, "202501")
    assert bq.kinds()[-1] == "delete"


def test_html_page_in_the_landing_zone_is_rejected_before_touching_bigquery():
    bq = FakeBQ()
    with pytest.raises(ValueError, match="nomes de coluna inválidos"):
        loader(bq, {URI: b"<html;x>\r\n<body;y>\r\n"}).load_inf_diario(URI, "202501")
    assert not bq.calls


def test_csv_with_plausible_header_but_wrong_columns_is_rejected_before_touching_bigquery():
    bq = FakeBQ()
    with pytest.raises(ValueError, match="não parece um CSV da CVM"):
        loader(bq, {URI: latin1_csv("nome;idade", ["ana;30"])}).load_inf_diario(URI, "202501")
    assert not bq.calls


def test_shifted_row_aborts_before_creating_anything():
    bq = FakeBQ()
    bad = latin1_csv(INF_NEW, [ROW, ROW + ";extra"])
    with pytest.raises(ValueError, match="campos"):
        loader(bq, {URI: bad}).load_inf_diario(URI, "202501")
    assert not bq.calls


def test_cad_fi_loads_a_daily_snapshot_with_its_own_partition_column():
    uri = "gs://bucket/cvm/cad_fi/data=20261012/cad_fi.csv"
    cad = latin1_csv("SIT;CNPJ_FUNDO;DENOM_SOCIAL", ["EM FUNCIONAMENTO NORMAL;00.000.000/0001-91;GESTÃO"])
    bq = FakeBQ(existing=["sit", "cnpj_fundo", "denom_social"])
    assert loader(bq, {uri: cad}).load_cad_fi(uri, "20261012") == {"rows": 1, "columns": 3}
    script = next(s for s in bq.sqls() if s.startswith("BEGIN TRANSACTION"))
    assert "`_src_snapshot` = DATE '2026-10-12'" in script and "`meu-projeto-gcp.raw.cad_fi`" in script


def test_invalid_names_are_rejected_up_front():
    for project in ["", "UPPER", "a b", "x;drop", "ab"]:
        with pytest.raises(ValueError):
            lb.RawLoader(FakeBQ(), project)
    with pytest.raises(ValueError):
        lb.RawLoader(FakeBQ(), "meu-projeto", dataset="raw; drop")
    with pytest.raises(ValueError):
        loader(FakeBQ(), {}).load_inf_diario(URI, "2025-01")


def test_local_reader_reads_files(tmp_path):
    f = tmp_path / "x.csv"
    f.write_bytes(b"abc")
    assert lb.Reader().read(str(f)) == b"abc"
