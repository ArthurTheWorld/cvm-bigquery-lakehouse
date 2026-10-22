"""
Carrega arquivos do landing zone (GCS ou disco) nas tabelas RAW do BigQuery.

Decisões (e por quê):
  * Tudo STRING. O raw nunca rejeita linha por causa de tipo; tipar é papel do dbt.
  * Colunas por NOME, não por posição: a ordem das colunas do cad_fi no arquivo
    não é a do dicionário oficial (alfabética).
  * O arquivo é lido em Python, validado, convertido de Latin-1 para UTF-8 e
    recebe `_src_row` (nº da linha). Um load job do BigQuery NÃO preserva a ordem do
    arquivo; sem isso, entre duas linhas com a mesma chave (reapresentação) não
    haveria como saber qual é a última.
  * Idempotente por partição: DELETE da partição + INSERT numa transação. Recarregar
    o mesmo mês (o arquivo do mês corrente muda todo dia útil) nunca duplica.
  * Mudança de schema tolerada: coluna nova no arquivo vira `ADD COLUMN IF NOT EXISTS`;
    coluna que sumiu do arquivo fica NULL nas linhas novas.
  * Metadados usam o prefixo `_src_`: o BigQuery RESERVA colunas que começam com
    `_FILE_`, `_TABLE_` e `_PARTITION`.

Tabelas:
  raw.inf_diario  particionada por mês  (_src_month)    uma carga por arquivo mensal
  raw.cad_fi      particionada por dia  (_src_snapshot) um snapshot por dia, expira em N dias

NÃO validado contra um projeto BigQuery real (sem conta no desenvolvimento): os testes
usam um cliente falso e conferem o SQL gerado e a ordem das chamadas.
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import uuid
from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Tuple

import cvm_files as cf

COLUMN = re.compile(r"^[a-z][a-z0-9_]*$")
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT = re.compile(r"^[a-z][a-z0-9-]{3,61}[a-z0-9]$")


class LoadError(RuntimeError):
    pass


# ------------------------------------------------------------- validação
def ident(value: str, what: str) -> str:
    if not IDENT.match(value or ""):
        raise ValueError(f"{what} inválido: {value!r}")
    return value


def project_id(value: str) -> str:
    if not PROJECT.match(value or ""):
        raise ValueError(f"project id inválido: {value!r}")
    return value


# ------------------------------------------------------------ CSV -> CSV
def normalize_csv(data: bytes) -> Tuple[List[str], bytes, int]:
    """Latin-1 com ';' -> UTF-8 com ',', mais a coluna `_src_row` (1, 2, 3...).

    Devolve (colunas do arquivo, bytes, nº de linhas de dados). Falha — em vez de
    deslocar colunas em silêncio — se alguma linha tiver outro nº de campos.
    """
    reader = csv.reader(io.StringIO(cf.decode(data), newline=""), delimiter=cf.SEP)
    try:
        header = [c.strip().lower() for c in next(reader)]
    except StopIteration:
        raise ValueError("arquivo vazio")
    bad = [c for c in header if not COLUMN.match(c)]
    if bad:
        raise ValueError(f"nomes de coluna inválidos no cabeçalho: {bad}")
    if len(set(header)) != len(header):
        raise ValueError("cabeçalho com colunas repetidas")

    out = io.StringIO()
    writer = csv.writer(out, delimiter=",", lineterminator="\n")
    writer.writerow(["_src_row"] + header)
    count = 0
    for line_no, row in enumerate(reader, start=2):
        if not row or (len(row) == 1 and row[0] == ""):
            continue  # linha em branco (tipicamente a última)
        if len(row) != len(header):
            raise ValueError(f"linha {line_no}: {len(row)} campos, o cabeçalho tem {len(header)}")
        count += 1
        writer.writerow([count] + row)
    return header, out.getvalue().encode("utf-8"), count


# -------------------------------------------------------------------- SQL
def q(*parts: str) -> str:
    return "`" + ".".join(parts) + "`"


def create_table_sql(fq: str, columns: List[str], kind: str, expiration_days: int = 120) -> str:
    cols = [f"`{c}` STRING" for c in columns] + ["`_src_row` INT64"]
    if kind == "inf_diario":
        meta = ["`_src_month` DATE", "`_src_file` STRING", "`_loaded_at` TIMESTAMP"]
        tail = "PARTITION BY DATE_TRUNC(`_src_month`, MONTH)"
    elif kind == "cad_fi":
        meta = ["`_src_snapshot` DATE", "`_src_file` STRING", "`_loaded_at` TIMESTAMP"]
        tail = f"PARTITION BY `_src_snapshot`\nOPTIONS (partition_expiration_days = {int(expiration_days)})"
    else:
        raise ValueError(f"tipo desconhecido: {kind!r}")
    body = ",\n  ".join(cols + meta)
    return f"CREATE TABLE IF NOT EXISTS {fq} (\n  {body}\n)\n{tail}"


def add_column_sql(fq: str, column: str) -> str:
    return f"ALTER TABLE {fq} ADD COLUMN IF NOT EXISTS `{column}` STRING"


def replace_partition_script(fq_target: str, fq_temp: str, columns: List[str], kind: str, partition: date, source_uri: str) -> str:
    part_col = "_src_month" if kind == "inf_diario" else "_src_snapshot"
    uri = source_uri.replace("\\", "\\\\").replace("'", "\\'")
    insert_cols = ", ".join(f"`{c}`" for c in columns + ["_src_row", part_col, "_src_file", "_loaded_at"])
    select_cols = ", ".join(f"`{c}`" for c in columns + ["_src_row"])
    return (
        "BEGIN TRANSACTION;\n"
        f"DELETE FROM {fq_target} WHERE `{part_col}` = DATE '{partition.isoformat()}';\n"
        f"INSERT INTO {fq_target} ({insert_cols})\n"
        f"SELECT {select_cols}, DATE '{partition.isoformat()}', '{uri}', CURRENT_TIMESTAMP()\n"
        f"FROM {fq_temp};\n"
        "COMMIT TRANSACTION;"
    )


def count_sql(fq_target: str, kind: str, partition: date) -> str:
    part_col = "_src_month" if kind == "inf_diario" else "_src_snapshot"
    return f"SELECT COUNT(*) AS n FROM {fq_target} WHERE `{part_col}` = DATE '{partition.isoformat()}'"


# ----------------------------------------------------------- dependências
def default_config_factory(columns: List[str]):
    from google.cloud import bigquery  # import tardio: os testes não precisam da biblioteca

    schema = [bigquery.SchemaField("_src_row", "INT64")] + [bigquery.SchemaField(c, "STRING") for c in columns]
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records=0,
        encoding="UTF-8",
    )


class Reader:
    """Lê `gs://bucket/chave` (GCS) ou um caminho local."""

    def __init__(self, storage_client=None):
        self._storage = storage_client

    def read(self, uri: str) -> bytes:
        if uri.startswith("gs://"):
            if self._storage is None:
                from google.cloud import storage

                self._storage = storage.Client()
            bucket, _, key = uri[5:].partition("/")
            return self._storage.bucket(bucket).blob(key).download_as_bytes()
        with open(uri, "rb") as handle:
            return handle.read()


# ----------------------------------------------------------------- loader
class RawLoader:
    def __init__(
        self,
        client,
        project: str,
        dataset: str = "raw",
        reader: Optional[Reader] = None,
        config_factory: Callable = default_config_factory,
        snapshot_expiration_days: int = 120,
        run_id: Optional[str] = None,
        now: Callable[[], datetime] = datetime.utcnow,
    ):
        self.client = client
        self.project, self.dataset = project_id(project), ident(dataset, "dataset")
        self.reader = reader or Reader()
        self.config_factory = config_factory
        self.expiration_days = snapshot_expiration_days
        self.run_id = run_id or uuid.uuid4().hex[:8]

    # API pública ------------------------------------------------------
    def load_inf_diario(self, uri: str, yyyymm: str) -> Dict[str, int]:
        month = date(int(cf.validate_month(yyyymm)[:4]), int(yyyymm[4:]), 1)
        return self._load("inf_diario", "inf_diario", uri, month, cf.INF_DIARIO_REQUIRED)

    def load_cad_fi(self, uri: str, yyyymmdd: str) -> Dict[str, int]:
        snapshot = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        return self._load("cad_fi", "cad_fi", uri, snapshot, cf.CAD_FI_REQUIRED)

    # interno ----------------------------------------------------------
    def _load(self, kind: str, table: str, uri: str, partition: date, required) -> Dict[str, int]:
        columns, payload, expected = normalize_csv(self.reader.read(uri))
        cf.require_columns(columns, required, table)

        target = f"{self.project}.{self.dataset}.{table}"
        temp = f"{self.project}.{self.dataset}._tmp_{table}_{self.run_id}"
        try:
            self.client.load_table_from_file(io.BytesIO(payload), temp, job_config=self.config_factory(columns)).result()

            self.client.query(create_table_sql(q(*target.split(".")), columns, kind, self.expiration_days)).result()
            existing = {field.name for field in self.client.get_table(target).schema}
            for column in columns:
                if column not in existing:
                    self.client.query(add_column_sql(q(*target.split(".")), column)).result()

            script = replace_partition_script(q(*target.split(".")), q(*temp.split(".")), columns, kind, partition, uri)
            self.client.query(script).result()

            loaded = list(self.client.query(count_sql(q(*target.split(".")), kind, partition)).result())[0][0]
            if loaded != expected:
                raise LoadError(f"{table} {partition}: o arquivo tem {expected} linhas, a partição ficou com {loaded}")
            return {"rows": loaded, "columns": len(columns)}
        finally:
            self.client.delete_table(temp, not_found_ok=True)  # nunca deixa tabela temporária para trás


# -------------------------------------------------------------------- CLI
def main() -> None:
    from fetch_cvm import cad_fi_key, inf_diario_key

    p = argparse.ArgumentParser(description="Carrega o landing zone (GCS) no BigQuery raw.")
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", default="raw")
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="cvm")
    p.add_argument("--months", nargs="*", default=[], help="AAAAMM do informe diário")
    p.add_argument("--cad-date", help="AAAAMMDD do snapshot do cad_fi")
    a = p.parse_args()

    from google.cloud import bigquery

    loader = RawLoader(bigquery.Client(project=a.project), a.project, a.dataset)
    base = f"gs://{a.bucket}/{a.prefix.strip('/')}"
    for month in a.months:
        print(month, loader.load_inf_diario(f"{base}/{inf_diario_key(month)}", month))
    if a.cad_date:
        snap = datetime.strptime(a.cad_date, "%Y%m%d").date()
        print(a.cad_date, loader.load_cad_fi(f"{base}/{cad_fi_key(snap)}", a.cad_date))


if __name__ == "__main__":
    main()
