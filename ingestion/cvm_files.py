"""
Acesso aos arquivos abertos da CVM (Portal de Dados Abertos, licença ODbL).

Lido nos dicionários de dados e na listagem oficial do portal:
  - informe diário: um ZIP por mês com um CSV; os 12 meses mais recentes ficam em DADOS/,
    os mais antigos em DADOS/HIST/ (os anteriores a 2021, por ano)
  - os CSVs são ISO-8859-1 (Latin-1), separados por ';'
  - o cadastro (cad_fi.csv) é um CSV simples, substituído periodicamente; o dicionário lista
    os campos em ordem alfabética, o que NÃO garante a ordem das colunas no arquivo: sempre
    ler por NOME de coluna

PREMISSA DE PROJETO (não confirmada em documentação): o arquivo do mês corrente muda ao longo
do mês e fundos podem reapresentar informes de dias anteriores. Por isso o pipeline recarrega o
mês inteiro a cada execução; se a premissa for falsa, isso só custa uma recarga desnecessária.

NÃO verificado contra o servidor real (o ambiente de desenvolvimento não
alcança dados.cvm.gov.br): os padrões exatos de URL de DADOS/HIST abaixo vêm da
documentação e de repositórios públicos. Por isso `inf_diario_candidates`
devolve VÁRIAS URLs e `fetch_first` tenta uma a uma.
"""
from __future__ import annotations

import io
import re
import zipfile
from datetime import date
from typing import Dict, List, Optional, Protocol, Tuple

import pandas as pd

BASE = "https://dados.cvm.gov.br/dados/FI"
CAD_FI_URL = f"{BASE}/CAD/DADOS/cad_fi.csv"
ENCODING = "latin-1"  # ISO-8859-1
SEP = ";"

_YYYYMM = re.compile(r"^\d{4}(0[1-9]|1[0-2])$")


class DownloadError(RuntimeError):
    pass


class Session(Protocol):
    def get(self, url: str, **kwargs): ...


# --------------------------------------------------------------------- meses
def validate_month(yyyymm: str) -> str:
    if not _YYYYMM.match(yyyymm or ""):
        raise ValueError(f"mês inválido: {yyyymm!r} (use AAAAMM, ex.: 202401)")
    return yyyymm


def months_back(reference: date, count: int) -> List[str]:
    """Os `count` meses terminando no mês de `reference` (inclusive), do mais antigo ao mais novo."""
    if count < 1:
        raise ValueError("count deve ser >= 1")
    year, month = reference.year, reference.month
    months = []
    for _ in range(count):
        months.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return months[::-1]


def month_range(start: str, end: str) -> List[str]:
    validate_month(start), validate_month(end)
    if start > end:
        raise ValueError("início depois do fim")
    out, year, month = [], int(start[:4]), int(start[4:])
    while f"{year:04d}{month:02d}" <= end:
        out.append(f"{year:04d}{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return out


# ----------------------------------------------------------------------- URLs
def inf_diario_candidates(yyyymm: str) -> List[str]:
    """URLs onde o informe diário de `yyyymm` pode estar, da mais provável para a menos."""
    validate_month(yyyymm)
    year = yyyymm[:4]
    base = f"{BASE}/DOC/INF_DIARIO/DADOS"
    urls = [f"{base}/inf_diario_fi_{yyyymm}.zip", f"{base}/HIST/inf_diario_fi_{yyyymm}.zip"]
    if int(year) <= 2020:
        urls.append(f"{base}/HIST/inf_diario_fi_{year}.zip")  # um ZIP por ano
    return urls


# ------------------------------------------------------------------- download
def download(url: str, session: Session, timeout: int = 120) -> Optional[bytes]:
    """Bytes do arquivo, ou None se 404. Qualquer outro erro HTTP levanta DownloadError
    (não confundir "não existe aqui" com "o servidor está com problema")."""
    response = session.get(url, timeout=timeout)
    if response.status_code == 404:
        return None
    if response.status_code != 200:
        raise DownloadError(f"{url} -> HTTP {response.status_code}")
    return response.content


def fetch_first(urls: List[str], session: Session) -> Tuple[str, bytes]:
    for url in urls:
        data = download(url, session)
        if data is not None:
            return url, data
    raise DownloadError("nenhuma URL candidata existe: " + ", ".join(urls))


# ------------------------------------------------------------------------ ZIP
def extract_csvs(zip_bytes: bytes) -> Dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        return {name: archive.read(name) for name in archive.namelist() if name.lower().endswith(".csv")}


def pick_month_csv(csvs: Dict[str, bytes], yyyymm: str) -> bytes:
    """O ZIP mensal tem 1 CSV; o ZIP anual (HIST) tem 12. Devolve o do mês pedido."""
    matches = [name for name in csvs if yyyymm in name]
    if len(matches) != 1:
        raise DownloadError(f"esperava exatamente 1 CSV para {yyyymm}, achei {sorted(csvs)}")
    return csvs[matches[0]]


# ------------------------------------------------------------------------ CSV
def decode(data: bytes) -> str:
    return data.decode(ENCODING)


def csv_header(data: bytes) -> List[str]:
    """Nomes das colunas (minúsculos, sem espaços) a partir da 1ª linha."""
    first_line = decode(data).split("\n", 1)[0].strip("\r\n")
    columns = [c.strip().strip('"').lower() for c in first_line.split(SEP)]
    if not first_line or any(not c for c in columns):
        raise ValueError(f"cabeçalho inválido: {first_line!r}")
    if len(set(columns)) != len(columns):
        raise ValueError(f"cabeçalho com colunas repetidas: {columns}")
    return columns


# Cada item é um grupo "qualquer um destes": o informe antigo usa CNPJ_FUNDO e o
# novo (Resolução CVM 175) usa CNPJ_FUNDO_CLASSE.
INF_DIARIO_REQUIRED = [{"dt_comptc"}, {"vl_quota"}, {"cnpj_fundo_classe", "cnpj_fundo"}]
CAD_FI_REQUIRED = [{"cnpj_fundo"}, {"denom_social"}]


def require_columns(header: List[str], groups: List[set], what: str) -> None:
    """Um portal fora do ar costuma devolver uma página HTML com HTTP 200. Sem esta
    checagem ela viraria um 'CSV' de 1 coluna no landing zone."""
    missing = [sorted(group) for group in groups if not group & set(header)]
    if missing:
        raise ValueError(f"{what} não parece um CSV da CVM: faltam colunas {missing}; cabeçalho: {header[:5]}")


def read_csv_bytes(data: bytes) -> pd.DataFrame:
    """Tudo como texto (sem inferência de tipo nem 'NaN'), colunas em minúsculas."""
    frame = pd.read_csv(io.BytesIO(data), sep=SEP, encoding=ENCODING, dtype=str, keep_default_na=False)
    frame.columns = [c.strip().lower() for c in frame.columns]
    return frame


def normalize_cnpj(value: str) -> str:
    """'00.000.000/0001-91' -> '00000000000191' (só dígitos)."""
    return "".join(ch for ch in (value or "") if ch.isdigit())
