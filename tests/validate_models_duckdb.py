#!/usr/bin/env python
"""
Valida a LÓGICA dos modelos dbt executando-os em DuckDB sobre dados sintéticos com
"verdade" conhecida (ingestion/synthetic_cvm.py).

O raw é montado com o MESMO normalizador do carregador do BigQuery
(load_bigquery.normalize_csv) e com a mesma semântica: substituição de partição,
`ADD COLUMN` para coluna nova, NULL para vazio.

Cenários:
  fase 1  meses antigos + cadastro (dia 1)          -> build completo
  fase 2  meses novos                               -> build incremental
  fase 3  RECARGA de 2 meses (um já FECHADO) com cotas corrigidas ("reapresentação") e
          cadastro do dia 2 com uma coluna NOVA e uma coluna REMOVIDA -> build incremental
  ref     full-refresh sobre o mesmo raw

O que se confere:
  - incremental == full-refresh, e o que foi corrigido num mês fechado chegou ao fim
  - Silver/Gold == recálculo independente em pandas (linhas, cotas, fluxos, rentabilidade)
  - os testes `warn` do dbt acusam EXATAMENTE os defeitos injetados (truth.json)
  - nenhum CPF e nenhum nome de diretor existem no Silver/Gold (LGPD)
  - controle negativo (--negative-control): com um filtro incremental ingênuo (por mês),
    a correção do mês fechado é perdida e o harness detecta

NÃO cobre sintaxe/semântica específica do BigQuery (insert_overwrite, particionamento,
BIGNUMERIC). Aqui `insert_overwrite` vira `delete+insert` por chave e `partition_by` é removido.

Uso:
  python tests/validate_models_duckdb.py [--funds 400 --months 8 --seed 42] [--negative-control]
Requer: requirements-harness.txt (dbt-duckdb, duckdb, pandas, numpy, pyyaml).
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))
import cvm_files as cf  # noqa: E402
import load_bigquery as lb  # noqa: E402
import synthetic_cvm as sc  # noqa: E402

DBT_SRC = ROOT / "dbt_project"
BASE_TS = datetime(2026, 1, 1, 6, 0, 0)

NAIVE_FILTER = """    where _src_month >= (select coalesce(max(_src_month), cast('1900-01-01' as date)) from {{ this }})"""


# ------------------------------------------------------------------ raw em DuckDB
class DuckRaw:
    """Emula lb.RawLoader sobre DuckDB: mesma normalização, mesma semântica de partição."""

    def __init__(self, con):
        self.con = con
        con.execute("create schema if not exists raw")

    def _existing(self, table: str) -> List[str]:
        return [r[0] for r in self.con.execute(
            "select column_name from information_schema.columns where table_schema='raw' and table_name=? order by ordinal_position",
            [table]).fetchall()]

    def _load(self, table: str, kind: str, data: bytes, partition: date, source: str, loaded_at: datetime) -> None:
        columns, payload, _ = lb.normalize_csv(data)
        part_col = "_src_month" if kind == "inf_diario" else "_src_snapshot"
        existing = self._existing(table)
        if not existing:
            cols = ", ".join(f'"{c}" varchar' for c in columns)
            self.con.execute(f'create table raw."{table}" ({cols}, "_src_row" bigint, "{part_col}" date, "_src_file" varchar, "_loaded_at" timestamp)')
        else:
            for c in columns:
                if c not in existing:  # coluna nova no arquivo -> ALTER TABLE ADD COLUMN IF NOT EXISTS
                    self.con.execute(f'alter table raw."{table}" add column "{c}" varchar')
        self.con.execute(f'delete from raw."{table}" where "{part_col}" = ?', [partition])
        df = pd.read_csv(io.BytesIO(payload), dtype=str, keep_default_na=False)
        df = df.replace({"": None})  # campo vazio sem aspas -> NULL, como no load job do BigQuery
        df["_src_row"] = df["_src_row"].astype("int64")
        self.con.register("df_in", df)
        names = ", ".join(f'"{c}"' for c in columns + ["_src_row"])
        self.con.execute(
            f'insert into raw."{table}" ({names}, "{part_col}", "_src_file", "_loaded_at") '
            f"select {names}, ?, ?, ? from df_in", [partition, source, loaded_at])
        self.con.unregister("df_in")

    def load_inf_diario(self, data: bytes, yyyymm: str, loaded_at: datetime) -> None:
        self._load("inf_diario", "inf_diario", data, date(int(yyyymm[:4]), int(yyyymm[4:]), 1), f"inf_diario_{yyyymm}.csv", loaded_at)

    def load_cad_fi(self, data: bytes, yyyymmdd: str, loaded_at: datetime) -> None:
        self._load("cad_fi", "cad_fi", data, datetime.strptime(yyyymmdd, "%Y%m%d").date(), f"cad_fi_{yyyymmdd}.csv", loaded_at)


# --------------------------------------------------------------------- dbt (cópia)
def make_workdir(tmp: Path, db_path: Path, naive: bool) -> Path:
    work = tmp / "dbt_project"
    shutil.copytree(DBT_SRC, work, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages", ".user.yml"))
    for sql in (work / "models").rglob("*.sql"):
        text = sql.read_text().replace("incremental_strategy='insert_overwrite'", "incremental_strategy='delete+insert'")
        # partition_by (dicionário do BigQuery) significa outra coisa no dbt-duckdb: só na cópia do teste
        text = re.sub(r"\s*partition_by=\{[^}]*\},", "", text)
        if naive and sql.name == "stg_inf_diario.sql":
            text = text.replace("{{ incremental_loaded_filter() }}", "{% if is_incremental() %}\n" + NAIVE_FILTER + "\n{% endif %}")
        sql.write_text(text)
    profiles = {"cvm_fundos": {"target": "dev", "outputs": {"dev": {
        "type": "duckdb", "path": str(db_path), "threads": 2,
        "settings": {"memory_limit": "1500MB", "preserve_insertion_order": False}}}}}
    (work / "profiles.yml").write_text(yaml.safe_dump(profiles))
    return work


def dbt_build(work: Path, label: str, *extra: str, retries: int = 2) -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(work), "GCP_PROJECT": "harness"}
    binary = str(Path(sys.executable).parent / "dbt")
    for attempt in range(retries + 1):
        r = subprocess.run([binary, "build", "--project-dir", str(work), *extra], env=env, capture_output=True, text=True)
        if r.returncode >= 0 or attempt == retries:
            break  # rc < 0: processo morto por sinal (DuckDB em pouca RAM); um build interrompido é seguro de repetir
        print(f"[{label}] processo abortado (rc={r.returncode}); repetindo ({attempt + 1}/{retries})")
    summary = [re.sub(r"\x1b\[[0-9;]*m", "", l).strip() for l in r.stdout.splitlines() if "Done." in l or " ERROR " in l or "FAIL " in l]
    print(f"[{label}] exit={r.returncode}  {' | '.join(s.split(' ', 1)[-1] for s in summary)}")
    if r.returncode != 0:
        print(r.stdout[-3500:], r.stderr[-1500:])
        sys.exit(f"dbt build falhou em: {label}")


def test_failures(work: Path) -> Dict[str, int]:
    results = json.loads((work / "target" / "run_results.json").read_text())["results"]
    return {r["unique_id"].split(".")[2]: int(r.get("failures") or 0) for r in results if r["unique_id"].startswith("test.")}


# ------------------------------------------------------------------- dados
def unify(files: Dict[str, bytes]) -> pd.DataFrame:
    """Todos os meses num só DataFrame, nomes novos de coluna, CNPJ só dígitos (recálculo em pandas)."""
    parts = []
    for month, data in sorted(files.items()):
        f = cf.read_csv_bytes(data).rename(columns={"tp_fundo": "tp_fundo_classe", "cnpj_fundo": "cnpj_fundo_classe"})
        if "id_subclasse" not in f:
            f["id_subclasse"] = ""
        parts.append(f.assign(month=month, order=range(len(f))))
    d = pd.concat(parts, ignore_index=True)
    d["fundo_cnpj"] = d["cnpj_fundo_classe"].map(cf.normalize_cnpj)
    return d


def expected_staging(files: Dict[str, bytes]) -> pd.DataFrame:
    d = unify(files).sort_values(["month", "order"], kind="stable")
    d = d.drop_duplicates(["fundo_cnpj", "id_subclasse", "dt_comptc"], keep="last")
    d["q"] = d["vl_quota"].astype(float)
    return d


def corrupt(data: bytes, rows: int, seed: int, factor: float = 1.01) -> Tuple[bytes, int]:
    """Reapresentação: altera a cota de `rows` linhas (sem criar saltos de 50%)."""
    f = cf.read_csv_bytes(data)
    idx = f.sample(n=min(rows, len(f)), random_state=seed).index
    f.loc[idx, "vl_quota"] = f.loc[idx, "vl_quota"].astype(float).map(lambda v: f"{v * factor:.12f}" if v > 0 else f"{v:.12f}")
    buf = io.StringIO()
    f.to_csv(buf, sep=";", index=False, lineterminator="\r\n")
    return buf.getvalue().encode("latin-1"), len(idx)


def change_cadastro(data: bytes) -> bytes:
    """Snapshot do dia seguinte: REMOVE classe_anbima e ACRESCENTA uma coluna nova."""
    f = cf.read_csv_bytes(data).drop(columns=["classe_anbima"])
    f["campo_novo_da_cvm"] = "x"
    buf = io.StringIO()
    f.to_csv(buf, sep=";", index=False, lineterminator="\r\n")
    return buf.getvalue().encode("latin-1")


# ---------------------------------------------------------------- cenário
def snapshot(con) -> Dict[str, float]:
    one = lambda sql: con.execute(sql).fetchone()[0]
    f = lambda v: float(v or 0)
    return {
        "stg_rows": one("select count(*) from staging.stg_inf_diario"),
        "stg_quota_sum": f(one("select sum(cast(vl_quota as double)) from staging.stg_inf_diario")),
        "stg_pl_sum": f(one("select sum(cast(vl_patrim_liq as double)) from staging.stg_inf_diario")),
        "fct_rows": one("select count(*) from marts.fct_fundo_dia"),
        "fct_flow_sum": f(one("select sum(cast(captacao_liquida as double)) from marts.fct_fundo_dia")),
        "fct_quota_sum": f(one("select sum(cast(vl_quota as double)) from marts.fct_fundo_dia")),
        "mensal_rows": one("select count(*) from marts.agg_fundo_mensal"),
        "mensal_pl_sum": f(one("select sum(cast(pl_final as double)) from marts.agg_fundo_mensal")),
        "mensal_ret_sum": f(one("select sum(coalesce(rentabilidade_mes, 0)) from marts.agg_fundo_mensal")),
        "mercado_pl_sum": f(one("select sum(cast(pl_total as double)) from marts.agg_mercado_mensal")),
        "gestora_pl_sum": f(one("select sum(cast(pl_total as double)) from marts.agg_gestora_mensal")),
        "dim_fundos": one("select count(*) from marts.dim_fundos"),
        "dim_classe_anbima_null": one("select count(*) from marts.dim_fundos where classe_anbima is null"),
    }


def same(a: Dict, b: Dict, key: str) -> bool:
    return abs(a[key] - b[key]) <= max(0.01, abs(b[key]) * 1e-9)


def run_scenario(files: Dict[str, bytes], cad1: bytes, naive: bool, label: str, keep_open: bool = False):
    months = sorted(files)
    old, new = months[:-2], months[-2:]
    closed, current = new  # `closed` já terminou quando chega a correção; `current` é o mês corrente

    tmp_ctx = tempfile.TemporaryDirectory()
    tmp = Path(tmp_ctx.name)
    db = tmp / "validate.duckdb"
    work = make_workdir(tmp, db, naive)
    ts = lambda h: BASE_TS + timedelta(hours=h)

    def db_load(fn):
        con = duckdb.connect(str(db))
        fn(DuckRaw(con))
        con.close()

    # fase 1: meses antigos + cadastro do dia 1
    def p1(raw):
        for i, m in enumerate(old):
            raw.load_inf_diario(files[m], m, ts(i))
        raw.load_cad_fi(cad1, "20261001", ts(0))
    db_load(p1)
    dbt_build(work, f"{label} | fase 1: carga inicial (full)")

    # fase 2: dois meses novos
    def p2(raw):
        for i, m in enumerate(new):
            raw.load_inf_diario(files[m], m, ts(100 + i))
    db_load(p2)
    dbt_build(work, f"{label} | fase 2: meses novos (incremental)")

    # fase 3: reapresentação em um mês FECHADO e no corrente; cadastro do dia 2 com schema diferente
    fixed = dict(files)
    fixed[closed], n1 = corrupt(files[closed], 60, 1)
    fixed[current], n2 = corrupt(files[current], 60, 2)

    def p3(raw):
        raw.load_inf_diario(fixed[closed], closed, ts(200))
        raw.load_inf_diario(fixed[current], current, ts(201))
        raw.load_cad_fi(change_cadastro(cad1), "20261002", ts(200))
    db_load(p3)
    dbt_build(work, f"{label} | fase 3: correções + mudança de schema do cadastro (incremental)")

    con = duckdb.connect(str(db))
    incremental = snapshot(con)
    con.close()
    dbt_build(work, f"{label} | referência: full-refresh", "--full-refresh")
    con = duckdb.connect(str(db), read_only=True)
    full = snapshot(con)
    return incremental, full, fixed, con, work, tmp_ctx


# --------------------------------------------------------------------- checks
def ok(name: str, passed: bool, detail: str = "") -> bool:
    print(f"  {'OK  ' if passed else 'ERRO'} {name}" + (f": {detail}" if detail else ""))
    return passed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--funds", type=int, default=400)
    ap.add_argument("--months", type=int, default=8)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--negative-control", action="store_true")
    a = ap.parse_args()

    gen = tempfile.TemporaryDirectory()
    cutover = pd.period_range("2025-01", periods=a.months, freq="M")[3].strftime("%Y%m")  # nomes antigos nos 3 primeiros meses
    truth = sc.generate(Path(gen.name), a.funds, a.months, "202501", a.seed, cutover)
    files = {p.parent.name.split("=")[1]: p.read_bytes() for p in sorted(Path(gen.name).glob("inf_diario/*/*.csv"))}
    cad1 = next(Path(gen.name).glob("cad_fi/*/cad_fi.csv")).read_bytes()
    print(f"dados sintéticos: {truth['informe_rows_in_files']:,} linhas em {len(files)} meses, {truth['funds_in_cadastro']} fundos no cadastro\n")

    if a.negative_control:
        inc, full, fixed, con, work, ctx = run_scenario(files, cad1, True, "controle (filtro ingênuo por mês)")
        con.close()
        ctx.cleanup()
        diffs = [k for k in full if k.startswith(("stg", "fct", "mensal")) and not same(inc, full, k)]
        print("\n== CONTROLE NEGATIVO: filtro incremental ingênuo (por mês) ==")
        for k in diffs:
            print(f"  diverge {k}: incremental={inc[k]:.4f} full={full[k]:.4f}")
        print("RESULTADO:", "o teste DETECTA a perda da correção em mês fechado" if diffs else "PROBLEMA: não detectou nada")
        sys.exit(0 if diffs else 1)

    inc, full, fixed, con, work, ctx = run_scenario(files, cad1, False, "lookback + _loaded_at")
    all_ok = True

    print("\n== incremental == full-refresh ==")
    for k in full:
        all_ok &= ok(k, same(inc, full, k), f"incremental={inc[k]:.4f} full={full[k]:.4f}")

    print("\n== Silver/Gold == recálculo independente em pandas ==")
    exp = expected_staging(fixed)
    stg = con.execute("select fundo_cnpj, id_subclasse, cast(dt_comptc as varchar) dt, cast(vl_quota as double) q from staging.stg_inf_diario").fetchdf()
    all_ok &= ok("linhas do stg_inf_diario", len(stg) == len(exp), f"modelo={len(stg)} esperado={len(exp)}")
    merged = stg.merge(exp[["fundo_cnpj", "id_subclasse", "dt_comptc", "q"]], left_on=["fundo_cnpj", "id_subclasse", "dt"],
                       right_on=["fundo_cnpj", "id_subclasse", "dt_comptc"], suffixes=("_m", "_e"))
    wrong = (merged["q_m"] - merged["q_e"]).abs() > 1e-9
    all_ok &= ok("cada cota == a da ÚLTIMA linha do arquivo (reapresentação e correção em mês fechado)",
                 len(merged) == len(exp) and not wrong.any(), f"{int(wrong.sum())} divergentes de {len(merged)}")
    flow = float((exp["captc_dia"].replace("", "0").astype(float) - exp["resg_dia"].replace("", "0").astype(float)).sum())
    got_flow = con.execute("select sum(cast(captacao_liquida as double)) from marts.fct_fundo_dia").fetchone()[0]
    all_ok &= ok("soma da captação líquida", abs(flow - got_flow) < 1.0, f"modelo={got_flow:,.2f} esperado={flow:,.2f}")

    # rentabilidade mensal de 40 séries, recalculada à mão
    exp["mes"] = exp["dt_comptc"].str[:7]
    exp = exp.sort_values(["fundo_cnpj", "id_subclasse", "dt_comptc"])
    last = exp.groupby(["fundo_cnpj", "id_subclasse", "mes"]).tail(1)
    last = last.assign(q=last["q"].where(last["q"] > 0))
    series = sorted(set(zip(last["fundo_cnpj"], last["id_subclasse"])))[:: max(1, len(set(zip(last["fundo_cnpj"], last["id_subclasse"]))) // 40)]
    model = con.execute("select fundo_cnpj, id_subclasse, strftime(mes, '%Y-%m') mes, rentabilidade_mes from marts.agg_fundo_mensal").fetchdf()
    bad = checked = 0
    for fundo, sub in series:
        s = last[(last["fundo_cnpj"] == fundo) & (last["id_subclasse"] == sub)].reset_index(drop=True)
        for i in range(1, len(s)):
            consecutive = (pd.Period(s.loc[i, "mes"]) - pd.Period(s.loc[i - 1, "mes"])).n == 1
            want = (s.loc[i, "q"] / s.loc[i - 1, "q"] - 1) if consecutive and s.loc[i, "q"] > 0 and s.loc[i - 1, "q"] > 0 else None
            got = model[(model["fundo_cnpj"] == fundo) & (model["id_subclasse"] == sub) & (model["mes"] == s.loc[i, "mes"])]["rentabilidade_mes"]
            got = None if got.empty or pd.isna(got.iloc[0]) else float(got.iloc[0])
            checked += 1
            if (want is None) != (got is None) or (want is not None and abs(want - got) > 1e-9):
                bad += 1
                if bad <= 6:
                    print(f"      divergência {fundo} sub={sub!r} mes={s.loc[i, 'mes']}: esperado={want} modelo={got}")
    all_ok &= ok("rentabilidade mensal (séries amostradas, recalculadas à mão)", bad == 0 and checked > 100, f"{bad} divergentes de {checked}")

    print("\n== os testes do dbt acusam exatamente os defeitos injetados ==")
    fails = test_failures(work)
    all_ok &= ok("cota <= 0", fails["assert_quota_is_positive"] == truth["nonpositive_quota_rows"], f"dbt={fails['assert_quota_is_positive']} injetado={truth['nonpositive_quota_rows']}")
    all_ok &= ok("patrimônio negativo", fails["assert_patrimonio_not_negative"] == truth["negative_pl_rows"], f"dbt={fails['assert_patrimonio_not_negative']} injetado={truth['negative_pl_rows']}")
    all_ok &= ok("retornos diários fora de +-50%", fails["assert_daily_return_within_50pct"] == 2 * truth["spike_funds"], f"dbt={fails['assert_daily_return_within_50pct']} esperado={2 * truth['spike_funds']}")
    all_ok &= ok("fundos do informe sem cadastro", fails["assert_informe_funds_have_cadastro"] == truth["orphan_funds_in_informe"], f"dbt={fails['assert_informe_funds_have_cadastro']} injetado={truth['orphan_funds_in_informe']}")

    print("\n== tolerância a mudança de schema ==")
    cols = [r[0] for r in con.execute("select column_name from information_schema.columns where table_schema='raw' and table_name='inf_diario'").fetchall()]
    all_ok &= ok("raw acumulou os dois schemas do informe", {"cnpj_fundo", "cnpj_fundo_classe", "id_subclasse"} <= set(cols))
    all_ok &= ok("raw.cad_fi ganhou a coluna nova", "campo_novo_da_cvm" in [r[0] for r in con.execute("select column_name from information_schema.columns where table_schema='raw' and table_name='cad_fi'").fetchall()])
    all_ok &= ok("coluna REMOVIDA do cadastro vira NULL (último snapshot não a tem)", full["dim_classe_anbima_null"] == full["dim_fundos"], f"{full['dim_classe_anbima_null']} de {full['dim_fundos']}")

    print("\n== LGPD: nenhum CPF e nenhum diretor no Silver/Gold ==")
    raw_cad = cf.read_csv_bytes(cad1)
    pf = raw_cad[raw_cad["pf_pj_gestor"] == "PF"]
    cpfs = {cf.normalize_cnpj(v) for v in pf["cpf_cnpj_gestor"]}
    diretores = set(raw_cad["diretor"]) | set(pf["gestor"])
    found = []
    for schema in ("staging", "marts"):
        for (table,) in con.execute("select table_name from information_schema.tables where table_schema=?", [schema]).fetchall():
            text_cols = [c for (c, t) in con.execute("select column_name, data_type from information_schema.columns where table_schema=? and table_name=?", [schema, table]).fetchall() if t.upper().startswith("VARCHAR")]
            for c in text_cols:
                vals = {v for (v,) in con.execute(f'select distinct "{c}" from {schema}."{table}" where "{c}" is not null').fetchall()}
                if vals & cpfs or vals & diretores:
                    found.append(f"{schema}.{table}.{c}")
    all_ok &= ok("varredura de todos os campos de texto", not found and len(cpfs) > 5, f"{len(cpfs)} CPFs e {len(diretores)} nomes procurados; encontrados em: {found or 'lugar nenhum'}")
    schema_cols = {c for (c,) in con.execute("select column_name from information_schema.columns where table_schema in ('staging','marts')").fetchall()}
    all_ok &= ok("não existem colunas de CPF/diretor", not (schema_cols & {"diretor", "cpf_cnpj_gestor", "cpf", "gestor_documento"}))
    sample = pf.iloc[0]
    want_key = hashlib.sha256(cf.normalize_cnpj(sample["cpf_cnpj_gestor"]).encode()).hexdigest()
    keys = {v for (v,) in con.execute("select gestor_chave from marts.dim_fundos where gestor_tipo = 'PF'").fetchall()}
    all_ok &= ok("gestor_chave de PF == SHA-256 do CPF (verificado com hashlib)", want_key in keys)
    names = [v for (v,) in con.execute("select gestor_nome from marts.dim_fundos where gestor_tipo = 'PF' limit 5").fetchall()]
    all_ok &= ok("nome de gestor PF pseudonimizado", all(n.startswith("PESSOA FÍSICA ") for n in names), names[0])

    con.close()
    ctx.cleanup()
    gen.cleanup()
    print("\nRESULTADO:", "TUDO CONSISTENTE" if all_ok else "HÁ DIVERGÊNCIAS")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
