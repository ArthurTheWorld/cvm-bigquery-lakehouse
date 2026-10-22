"""
Gerador de dados SINTÉTICOS no schema oficial da CVM (informe diário + cad_fi),
para rodar o projeto sem acesso ao portal e para validar a lógica dos modelos.

Fidelidade ao formato real (dicionários de dados oficiais):
  - Latin-1, separador ';', CRLF, datas ISO, ponto decimal, VL_QUOTA com 12 casas
  - informe com o schema atual (TP_FUNDO_CLASSE, CNPJ_FUNDO_CLASSE, ID_SUBCLASSE...)
    e, opcionalmente, o antigo (TP_FUNDO, CNPJ_FUNDO) antes de `schema_cutover`
    para exercitar a tolerância a mudança de schema. O mês exato da virada aqui é
    ARBITRÁRIO e os nomes antigos seguem a memória do formato histórico: não foram
    conferidos contra arquivo real.
  - cad_fi com os 41 campos do dicionário, em ORDEM EMBARALHADA (o dicionário é
    alfabético e isso não garante a ordem do arquivo)

Os dados NÃO são reais: nomes, CNPJs e CPFs são fictícios (com dígitos
verificadores válidos, como em qualquer gerador de dados de teste).

Defeitos injetados, em conjuntos DISJUNTOS de fundos (assim a contagem esperada
de cada defeito é exata e fica registrada em truth.json):
  - duplicatas: o fundo "reapresenta" o informe de um dia (mesma chave, valor
    corrigido, linha posterior no arquivo)
  - cota <= 0, patrimônio líquido negativo
  - salto de cota (x2,2 por um dia): gera 2 retornos diários fora de +-50%
  - fundos presentes no informe e ausentes do cadastro ("órfãos")

Uso:
  python ingestion/synthetic_cvm.py --out data/raw --funds 1500 --months 14 --seed 42
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

CAD_FI_COLUMNS = [  # os 41 campos do dicionário oficial do cad_fi
    "ADMIN", "AUDITOR", "CD_CVM", "CLASSE", "CLASSE_ANBIMA", "CNPJ_ADMIN", "CNPJ_AUDITOR",
    "CNPJ_CONTROLADOR", "CNPJ_CUSTODIANTE", "CNPJ_FUNDO", "CONDOM", "CONTROLADOR",
    "CPF_CNPJ_GESTOR", "CUSTODIANTE", "DENOM_SOCIAL", "DIRETOR", "DT_CANCEL", "DT_CONST",
    "DT_FIM_EXERC", "DT_INI_ATIV", "DT_INI_CLASSE", "DT_INI_EXERC", "DT_INI_SIT",
    "DT_PATRIM_LIQ", "DT_REG", "ENTID_INVEST", "FUNDO_COTAS", "FUNDO_EXCLUSIVO", "GESTOR",
    "INF_TAXA_ADM", "INF_TAXA_PERFM", "INVEST_CEMPR_EXTER", "PF_PJ_GESTOR", "PUBLICO_ALVO",
    "RENTAB_FUNDO", "SIT", "TAXA_ADM", "TAXA_PERFM", "TP_FUNDO", "TRIB_LPRAZO", "VL_PATRIM_LIQ",
]
INF_NEW = ["TP_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC", "VL_TOTAL", "VL_QUOTA",
           "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST"]
INF_OLD = ["TP_FUNDO", "CNPJ_FUNDO", "DT_COMPTC", "VL_TOTAL", "VL_QUOTA", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST"]

BRANDS = ["ALFA", "BETA", "GAMA", "DELTA", "ÔMEGA", "SIGMA", "ATLÂNTICO", "HORIZONTE", "MERIDIANO", "PRIME"]
CLASSES = {  # CLASSE do cadastro -> (rótulo no nome, retorno diário médio, volatilidade)
    "Fundo de Renda Fixa": ("RENDA FIXA", 0.0004, 0.0008),
    "Fundo Multimercado": ("MULTIMERCADO", 0.0004, 0.0035),
    "Fundo de Ações": ("AÇÕES", 0.0005, 0.011),
    "Fundo Cambial": ("CAMBIAL", 0.0002, 0.006),
}
ANBIMA = {"Fundo de Renda Fixa": "Renda Fixa Duração Baixa Grau de Invest.", "Fundo Multimercado": "Multimercados Livre",
          "Fundo de Ações": "Ações Livre", "Fundo Cambial": "Cambial"}
SITUATIONS = [("EM FUNCIONAMENTO NORMAL", 0.86), ("CANCELADA", 0.10), ("LIQUIDAÇÃO", 0.04)]
ADMINS = [("ADMINISTRADORA ÁGUIA S.A.", 0), ("BANCO ORION S.A.", 1), ("CUSTÓDIA E ADMINISTRAÇÃO NORTE LTDA", 2)]
PERSON_FIRST = ["MARIA", "JOSÉ", "ANA", "CARLOS", "LÚCIA", "PAULO", "BEATRIZ", "ROBERTO"]
PERSON_LAST = ["SOUZA", "PEREIRA", "FERREIRA", "ALMEIDA", "RIBEIRO", "CARVALHO", "MARTINS", "ARAÚJO"]


# --------------------------------------------------------------- documentos
def _check_digit(digits: List[int], weights: List[int]) -> int:
    remainder = sum(d * w for d, w in zip(digits, weights)) % 11
    return 0 if remainder < 2 else 11 - remainder


def cnpj_from_base(base12: str) -> str:
    """12 dígitos (8 da raiz + 4 da filial) -> 'NN.NNN.NNN/NNNN-DD' com dígitos válidos."""
    digits = [int(c) for c in base12]
    d1 = _check_digit(digits, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = _check_digit(digits + [d1], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    s = base12 + f"{d1}{d2}"
    return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"


def cpf_from_base(base9: str) -> str:
    digits = [int(c) for c in base9]
    d1 = _check_digit(digits, list(range(10, 1, -1)))
    d2 = _check_digit(digits + [d1], list(range(11, 1, -1)))
    s = base9 + f"{d1}{d2}"
    return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"


def valid_cnpj(formatted: str) -> bool:
    digits = "".join(c for c in formatted if c.isdigit())
    return len(digits) == 14 and cnpj_from_base(digits[:12]) == formatted


# ---------------------------------------------------------------- geração
def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep=";", index=False, lineterminator="\r\n", encoding="latin-1")


def _funds(rng: np.random.Generator, n: int) -> pd.DataFrame:
    roots = rng.choice(np.arange(10_000_000, 99_999_999), size=n, replace=False)
    classes = rng.choice(list(CLASSES), size=n, p=[0.35, 0.35, 0.2, 0.1])
    gestor_pf = rng.random(n) < 0.15
    sit = rng.choice([s for s, _ in SITUATIONS], size=n, p=[p for _, p in SITUATIONS])
    funds = pd.DataFrame({
        "cnpj": [cnpj_from_base(f"{r:08d}0001") for r in roots],
        "classe": classes,
        "gestor_pf": gestor_pf,
        "sit": sit,
        "pl0": rng.lognormal(mean=16.5, sigma=1.4, size=n),
        "q0": rng.uniform(1.0, 12.0, size=n),
        "n_sub": np.where(rng.random(n) < 0.10, 2, 1),
    })
    brand = rng.choice(BRANDS, size=n)
    funds["nome"] = [f"{b} {CLASSES[c][0]} {i:04d} FI" for i, (b, c) in enumerate(zip(brand, funds["classe"]), start=1)]
    return funds


def _cadastro(rng: np.random.Generator, funds: pd.DataFrame, snapshot: date, first_day: date) -> pd.DataFrame:
    n = len(funds)
    admin_idx = rng.integers(0, len(ADMINS), size=n)
    gestor_name, gestor_doc = [], []
    for i in range(n):
        if funds.loc[i, "gestor_pf"]:
            gestor_name.append(f"{rng.choice(PERSON_FIRST)} {rng.choice(PERSON_LAST)} {rng.choice(PERSON_LAST)}")
            gestor_doc.append(cpf_from_base(f"{int(rng.integers(100_000_000, 999_999_999))}"))
        else:
            k = int(rng.integers(0, 40))
            gestor_name.append(f"GESTORA {BRANDS[k % len(BRANDS)]} {k:02d} LTDA")
            gestor_doc.append(cnpj_from_base(f"{20_000_000 + k:08d}0001"))
    cancelled = funds["sit"] != "EM FUNCIONAMENTO NORMAL"
    iso = lambda d: d.isoformat()
    cad = pd.DataFrame({
        "ADMIN": [ADMINS[i][0] for i in admin_idx],
        "AUDITOR": "AUDITORES ASSOCIADOS S/S",
        "CD_CVM": [str(100_000 + i) for i in range(n)],
        "CLASSE": funds["classe"],
        "CLASSE_ANBIMA": funds["classe"].map(ANBIMA),
        "CNPJ_ADMIN": [cnpj_from_base(f"{30_000_000 + ADMINS[i][1]:08d}0001") for i in admin_idx],
        "CNPJ_AUDITOR": cnpj_from_base("400000000001"),
        "CNPJ_CONTROLADOR": "",
        "CNPJ_CUSTODIANTE": cnpj_from_base("500000000001"),
        "CNPJ_FUNDO": funds["cnpj"],
        "CONDOM": np.where(rng.random(n) < 0.9, "Aberto", "Fechado"),
        "CONTROLADOR": "",
        "CPF_CNPJ_GESTOR": gestor_doc,
        "CUSTODIANTE": "CUSTODIANTE CENTRAL S.A.",
        "DENOM_SOCIAL": funds["nome"],
        "DIRETOR": [f"{rng.choice(PERSON_FIRST)} {rng.choice(PERSON_LAST)}" for _ in range(n)],
        "DT_CANCEL": np.where(cancelled, iso(snapshot), ""),
        "DT_CONST": iso(date(2015, 3, 1)),
        "DT_FIM_EXERC": "12-31",
        "DT_INI_ATIV": iso(date(2015, 6, 1)),
        "DT_INI_CLASSE": iso(date(2015, 6, 1)),
        "DT_INI_EXERC": "01-01",
        "DT_INI_SIT": iso(date(2020, 1, 2)),
        "DT_PATRIM_LIQ": iso(first_day),
        "DT_REG": iso(date(2015, 4, 1)),
        "ENTID_INVEST": "N",
        "FUNDO_COTAS": np.where(rng.random(n) < 0.12, "S", "N"),
        "FUNDO_EXCLUSIVO": np.where(rng.random(n) < 0.08, "S", "N"),
        "GESTOR": gestor_name,
        "INF_TAXA_ADM": "",
        "INF_TAXA_PERFM": "",
        "INVEST_CEMPR_EXTER": "N",
        "PF_PJ_GESTOR": np.where(funds["gestor_pf"], "PF", "PJ"),
        "PUBLICO_ALVO": rng.choice(["Publico em Geral", "Qualificado", "Profissional"], size=n, p=[0.6, 0.3, 0.1]),
        "RENTAB_FUNDO": "",
        "SIT": funds["sit"],
        "TAXA_ADM": [f"{v:.2f}" for v in rng.uniform(0.0, 2.5, size=n)],
        "TAXA_PERFM": np.where(rng.random(n) < 0.4, "20.00", ""),
        "TP_FUNDO": "FI",
        "TRIB_LPRAZO": np.where(funds["classe"] == "Fundo de Renda Fixa", "S", "N"),
        "VL_PATRIM_LIQ": [f"{v:.2f}" for v in funds["pl0"]],
    })
    assert list(cad.columns) == CAD_FI_COLUMNS
    return cad[list(rng.permutation(CAD_FI_COLUMNS))]  # ordem do ARQUIVO != ordem do dicionário


def generate(
    out_dir: Path,
    funds: int = 1500,
    months: int = 14,
    start_month: str = "202501",
    seed: int = 42,
    schema_cutover: str | None = None,
    snapshot: date | None = None,
    orphan_funds: int = 40,
) -> Dict:
    """Escreve os arquivos em `out_dir` (mesmo layout do fetch_cvm) e devolve a verdade conhecida."""
    rng = np.random.default_rng(seed)
    out_dir = Path(out_dir)
    f = _funds(rng, funds)

    month_keys = pd.period_range(start_month[:4] + "-" + start_month[4:], periods=months, freq="M")
    days = pd.bdate_range(month_keys[0].start_time, month_keys[-1].end_time)
    snapshot = snapshot or days[-1].date()
    n_days = len(days)

    # conjuntos disjuntos de fundos para cada defeito
    order = rng.permutation(funds)
    pos = 0
    sets: Dict[str, List[int]] = {}
    for name, share in [("duplicate", 0.03), ("nonpositive_quota", 0.01), ("negative_pl", 0.01), ("spike", 0.01)]:
        k = max(2, int(funds * share))
        sets[name] = [int(i) for i in order[pos:pos + k]]
        pos += k
    orphans = set(int(i) for i in order[pos:pos + orphan_funds])  # sem cadastro

    frames = []
    for i in range(funds):
        start = int(rng.integers(0, int(n_days * 0.3))) if rng.random() < 0.3 else 0
        end = n_days
        if f.loc[i, "sit"] != "EM FUNCIONAMENTO NORMAL":
            end = int(rng.integers(int(n_days * 0.5), n_days))
        idx = np.arange(start, end)
        if len(idx) < 25:
            continue
        mu, sigma = CLASSES[f.loc[i, "classe"]][1:]
        returns = rng.normal(mu, sigma, size=len(idx))
        quota = f.loc[i, "q0"] * np.cumprod(1 + returns)
        flows_in = np.where(rng.random(len(idx)) < 0.7, rng.uniform(0, 0.01, len(idx)), 0.0)
        flows_out = np.where(rng.random(len(idx)) < 0.7, rng.uniform(0, 0.01, len(idx)), 0.0)
        pl = np.empty(len(idx))
        level = f.loc[i, "pl0"]
        for t in range(len(idx)):
            level = max(level * (1 + returns[t]) * (1 + flows_in[t] - flows_out[t]), 1_000.0)
            pl[t] = level
        n_sub = int(f.loc[i, "n_sub"])
        for s in range(n_sub):
            share = 1.0 if n_sub == 1 else (0.6 if s == 0 else 0.4)
            frames.append(pd.DataFrame({
                "fund": i, "sub": ("" if n_sub == 1 else f"SUB{s + 1}"),
                "day": days[idx], "quota": quota, "pl": pl * share,
                "capt": flows_in * pl * share, "resg": flows_out * pl * share,
                "cotst": np.maximum(1, (rng.integers(50, 20_000) * (1 + np.cumsum(rng.normal(0, 0.002, len(idx))))).astype(int)),
            }))
    d = pd.concat(frames, ignore_index=True)
    d["total"] = d["pl"] * rng.uniform(1.0, 1.02, size=len(d))
    d["month"] = d["day"].dt.strftime("%Y%m")

    # No schema ANTIGO não existe ID_SUBCLASSE: um fundo com 2 subclasses geraria 2 linhas com a
    # MESMA chave. Nesses meses o fundo aparece como uma linha única (SUB1 com o total).
    if schema_cutover:
        old = d["month"] < schema_cutover
        d = d[~(old & (d["sub"] == "SUB2"))].copy()
        merge = (d["month"] < schema_cutover) & (d["sub"] == "SUB1")
        d.loc[merge, ["pl", "capt", "resg", "total"]] = d.loc[merge, ["pl", "capt", "resg", "total"]] / 0.6
        d.loc[merge, "sub"] = ""

    # ---- defeitos injetados ---------------------------------------------------
    truth: Dict = {"months": [str(m).replace("-", "") for m in month_keys], "schema_cutover": schema_cutover}
    d = d.reset_index(drop=True)

    def pick_rows(fund_ids: List[int], per_fund: int, avoid_month_edges: bool = True) -> List[int]:
        rows = []
        for fid in fund_ids:
            sub = d.index[(d["fund"] == fid) & (d["sub"].isin(["", "SUB1"]))]
            if len(sub) < 12:
                continue
            lo, hi = 4, len(sub) - 4
            for pos_ in rng.choice(np.arange(lo, hi), size=per_fund, replace=False):
                row = sub[pos_]
                if avoid_month_edges and d.loc[row, "day"].is_month_end:
                    continue
                # a série (fundo, subclasse) não pode mudar de rótulo ao redor da linha: os
                # retornos vizinhos precisam existir na MESMA série para a contagem ser exata
                if len({d.loc[sub[pos_ - 1], "sub"], d.loc[row, "sub"], d.loc[sub[pos_ + 1], "sub"]}) != 1:
                    continue
                rows.append(int(row))
        return rows

    q_rows = pick_rows(sets["nonpositive_quota"], 1)
    d.loc[q_rows, "quota"] = rng.choice([0.0, -1.5], size=len(q_rows))
    pl_rows = pick_rows(sets["negative_pl"], 1)
    d.loc[pl_rows, "pl"] = -abs(d.loc[pl_rows, "pl"])
    spike_rows = pick_rows(sets["spike"], 1)
    d.loc[spike_rows, "quota"] = d.loc[spike_rows, "quota"] * 2.2  # sobe 1 dia e volta: 2 retornos fora de +-50%
    dup_rows = pick_rows(sets["duplicate"], 3)

    fmt = pd.DataFrame({
        "DT_COMPTC": d["day"].dt.strftime("%Y-%m-%d"),
        "VL_TOTAL": d["total"].map("{:.2f}".format),
        "VL_QUOTA": d["quota"].map("{:.12f}".format),
        "VL_PATRIM_LIQ": d["pl"].map("{:.2f}".format),
        "CAPTC_DIA": d["capt"].map("{:.2f}".format),
        "RESG_DIA": d["resg"].map("{:.2f}".format),
        "NR_COTST": d["cotst"].astype(str),
    })
    blank = rng.random(len(fmt)) < 0.02
    fmt.loc[blank, "CAPTC_DIA"] = ""  # campo opcional vazio, como na fonte
    cnpjs = f["cnpj"].to_numpy()
    fmt["CNPJ"] = cnpjs[d["fund"].to_numpy()]
    fmt["SUB"] = d["sub"].to_numpy()
    fmt["TP"] = "FI"
    fmt["month"] = d["month"].to_numpy()

    # reapresentações: linha corrigida, posterior no arquivo, mesma chave
    corrections = fmt.loc[dup_rows].copy()
    corrected_quota = (d.loc[dup_rows, "quota"] * 1.0001).map("{:.12f}".format)
    corrections["VL_QUOTA"] = corrected_quota
    truth["corrected"] = [
        {"cnpj": r.CNPJ, "id_subclasse": r.SUB, "dt_comptc": r.DT_COMPTC, "vl_quota": r.VL_QUOTA}
        for r in corrections.head(25).itertuples()
    ]
    fmt = pd.concat([fmt, corrections], ignore_index=True)

    cutover = schema_cutover or "000000"
    for month, part in fmt.groupby("month", sort=True):
        part = part.drop(columns="month")
        if month >= cutover:
            frame = part.rename(columns={"TP": "TP_FUNDO_CLASSE", "CNPJ": "CNPJ_FUNDO_CLASSE", "SUB": "ID_SUBCLASSE"})[INF_NEW]
        else:
            frame = part.rename(columns={"TP": "TP_FUNDO", "CNPJ": "CNPJ_FUNDO"})[INF_OLD]
        _write_csv(frame, out_dir / f"inf_diario/ano_mes={month}/inf_diario_fi_{month}.csv")

    cad = _cadastro(rng, f, snapshot, days[0].date())
    orphan_cnpjs = set(f.loc[sorted(orphans), "cnpj"])  # existem no informe, mas não no cadastro
    cad = cad[~cad["CNPJ_FUNDO"].isin(orphan_cnpjs)].reset_index(drop=True)
    _write_csv(cad, out_dir / f"cad_fi/data={snapshot:%Y%m%d}/cad_fi.csv")

    informe_cnpjs = set(fmt["CNPJ"])
    truth.update({
        "informe_rows_in_files": int(len(fmt)),
        "informe_rows_after_dedupe": int(len(fmt) - len(dup_rows)),
        "duplicate_rows": len(dup_rows),
        "nonpositive_quota_rows": len(q_rows),
        "negative_pl_rows": len(pl_rows),
        "spike_funds": len(spike_rows),
        "funds_in_informe": len(informe_cnpjs),
        "funds_in_cadastro": int(len(cad)),
        "orphan_funds_in_informe": len(informe_cnpjs - set(cad["CNPJ_FUNDO"])),
        "snapshot": f"{snapshot:%Y%m%d}",
    })
    (out_dir / "truth.json").write_text(json.dumps(truth, indent=2, ensure_ascii=False), encoding="utf-8")
    return truth


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="data/raw")
    p.add_argument("--funds", type=int, default=1500)
    p.add_argument("--months", type=int, default=14)
    p.add_argument("--start-month", default="202501")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--schema-cutover", help="AAAAMM a partir do qual vale o schema novo (antes: nomes antigos)")
    a = p.parse_args()
    truth = generate(Path(a.out), a.funds, a.months, a.start_month, a.seed, a.schema_cutover)
    print(json.dumps({k: v for k, v in truth.items() if k != "corrected"}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
