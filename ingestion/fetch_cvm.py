"""
Baixa o informe diário (últimos N meses) e o cadastro de fundos da CVM e
deposita os arquivos ORIGINAIS (Latin-1, ';') no destino: nada é reescrito ou
convertido aqui, para o landing zone ser uma cópia fiel da fonte.

Layout (idêntico para disco e GCS, e igual ao que o gerador sintético produz):
  inf_diario/ano_mes=AAAAMM/inf_diario_fi_AAAAMM.csv
  cad_fi/data=AAAAMMDD/cad_fi.csv          (um snapshot por dia de execução)

Idempotente: se o arquivo de destino já tem exatamente o mesmo conteúdo, não
regrava. O mês corrente muda todo dia útil (a CVM acrescenta dias e aceita
reapresentações), então ele é regravado; meses fechados ficam como estão.

Uso:
  python ingestion/fetch_cvm.py --months-back 3 --target local --out data/raw
  python ingestion/fetch_cvm.py --months 202401 202402 --target gcs --bucket <bucket> --prefix cvm
"""
from __future__ import annotations

import argparse
import base64
import hashlib
from datetime import date
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional

import cvm_files as cf

WRITTEN, SKIPPED = "written", "skipped"


def inf_diario_key(yyyymm: str) -> str:
    return f"inf_diario/ano_mes={cf.validate_month(yyyymm)}/inf_diario_fi_{yyyymm}.csv"


def cad_fi_key(snapshot: date) -> str:
    return f"cad_fi/data={snapshot:%Y%m%d}/cad_fi.csv"


class LocalSink:
    def __init__(self, root: Path, prefix: str = ""):
        self.root, self.prefix = Path(root), prefix.strip("/")

    def put(self, key: str, data: bytes) -> str:
        path = self.root / self.prefix / key
        if path.exists() and path.read_bytes() == data:
            return SKIPPED
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return WRITTEN


class GcsSink:
    """`bucket` é um google.cloud.storage.Bucket."""

    def __init__(self, bucket, prefix: str = "cvm"):
        self.bucket, self.prefix = bucket, prefix.strip("/")

    def put(self, key: str, data: bytes) -> str:
        blob = self.bucket.blob(f"{self.prefix}/{key}" if self.prefix else key)
        if blob.exists():
            blob.reload()
            if blob.md5_hash == base64.b64encode(hashlib.md5(data).digest()).decode():
                return SKIPPED
        blob.upload_from_string(data, content_type="text/csv")
        return WRITTEN


def fetch(
    months: Iterable[str],
    sink,
    session,
    today: Optional[date] = None,
    include_cadastro: bool = True,
    log: Callable[[str], None] = print,
) -> Dict[str, Dict[str, str]]:
    """Devolve {'inf_diario': {AAAAMM: written|skipped}, 'cad_fi': {AAAAMMDD: ...}}."""
    today = today or date.today()
    summary: Dict[str, Dict[str, str]] = {"inf_diario": {}, "cad_fi": {}}

    for yyyymm in months:
        url, payload = cf.fetch_first(cf.inf_diario_candidates(yyyymm), session)
        csv_bytes = cf.pick_month_csv(cf.extract_csvs(payload), yyyymm)
        cf.require_columns(cf.csv_header(csv_bytes), cf.INF_DIARIO_REQUIRED, f"informe diário {yyyymm}")
        status = sink.put(inf_diario_key(yyyymm), csv_bytes)
        summary["inf_diario"][yyyymm] = status
        log(f"[inf_diario {yyyymm}] {status} <- {url}")

    if include_cadastro:
        _, data = cf.fetch_first([cf.CAD_FI_URL], session)
        cf.require_columns(cf.csv_header(data), cf.CAD_FI_REQUIRED, "cad_fi.csv")
        status = sink.put(cad_fi_key(today), data)
        summary["cad_fi"][f"{today:%Y%m%d}"] = status
        log(f"[cad_fi {today:%Y%m%d}] {status}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--months", nargs="+", help="meses AAAAMM")
    group.add_argument("--months-back", type=int, default=2, help="últimos N meses, incluindo o corrente (padrão 2)")
    parser.add_argument("--target", choices=["local", "gcs"], default="local")
    parser.add_argument("--out", default="data/raw", help="raiz de saída com --target local")
    parser.add_argument("--bucket", help="obrigatório com --target gcs")
    parser.add_argument("--prefix", default="", help="prefixo dentro do destino (ex.: cvm)")
    parser.add_argument("--skip-cadastro", action="store_true")
    args = parser.parse_args()

    months = args.months or cf.months_back(date.today(), args.months_back)
    if args.target == "gcs":
        if not args.bucket:
            parser.error("--bucket é obrigatório com --target gcs")
        from google.cloud import storage  # import tardio: o modo local não precisa

        sink = GcsSink(storage.Client().bucket(args.bucket), args.prefix or "cvm")
    else:
        sink = LocalSink(Path(args.out), args.prefix)

    import requests

    session = requests.Session()
    session.headers["User-Agent"] = "cvm-bigquery-lakehouse (portfolio; dados abertos)"
    fetch(months, sink, session, include_cadastro=not args.skip_cadastro)


if __name__ == "__main__":
    main()
