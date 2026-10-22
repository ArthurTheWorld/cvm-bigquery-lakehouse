"""Checagem de sintaxe e de POSTURA do Terraform sem precisar do binário (o CI roda
`terraform fmt -check` e `validate` de verdade).

O python-hcl2 mantém as aspas dos rótulos e das strings e devolve expressões como
`${...}`; o helper abaixo normaliza isso.
"""
from pathlib import Path

import hcl2
import pytest

TF_DIR = Path(__file__).parent.parent / "infra" / "terraform"
FILES = sorted(TF_DIR.glob("*.tf"))


def unq(value):
    return value.strip('"') if isinstance(value, str) else value


def load(name):
    with open(TF_DIR / name) as handle:
        return hcl2.load(handle)


def resources():
    out = {}
    for block in load("main.tf")["resource"]:
        for rtype, instances in block.items():
            for name, body in instances.items():
                out[f"{unq(rtype)}.{unq(name)}"] = body
    return out


def variables():
    return {unq(k): body for block in load("variables.tf")["variable"] for k, body in block.items()}


def test_files_found():
    assert {f.name for f in FILES} == {"main.tf", "outputs.tf", "variables.tf", "versions.tf"}


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_hcl_parses(path):
    assert load(path.name)


def test_composer_and_budget_are_opt_in():
    res = resources()
    assert "create_composer" in res["google_composer_environment.main"]["count"]
    assert "billing_account_id" in res["google_billing_budget.monthly"]["count"]
    assert variables()["create_composer"]["default"] is False      # caro: nunca liga sem querer
    assert unq(variables()["billing_account_id"]["default"]) == ""


def test_landing_bucket_is_private_and_expires_files():
    bucket = resources()["google_storage_bucket.landing"]
    assert unq(bucket["public_access_prevention"]) == "enforced"
    assert bucket["uniform_bucket_level_access"] is True
    assert bucket["lifecycle_rule"]


def test_reporter_can_only_read_the_marts_dataset():
    """O raw tem CPF e nome de diretor: o consumo só pode ler o marts."""
    res = resources()
    reporter = [k for k in res if k.startswith("google_bigquery_dataset_iam_member") and "reporter" in k]
    assert reporter == ["google_bigquery_dataset_iam_member.reporter_marts"]
    assert 'layers["marts"]' in res[reporter[0]]["dataset_id"]
    assert unq(res[reporter[0]]["role"]) == "roles/bigquery.dataViewer"


def test_ci_service_account_has_no_write_access_to_raw():
    res = resources()
    ci_on_raw = [k for k, v in res.items() if k.startswith("google_bigquery_dataset_iam_member") and "ci_" in k]
    assert ci_on_raw == ["google_bigquery_dataset_iam_member.ci_raw_viewer"]
    assert unq(res[ci_on_raw[0]]["role"]) == "roles/bigquery.dataViewer"


def test_only_the_pipeline_account_can_write_to_every_layer():
    editor = resources()["google_bigquery_dataset_iam_member.pipeline_editor"]
    assert "pipeline" in editor["member"] and unq(editor["role"]) == "roles/bigquery.dataEditor"
    assert "google_bigquery_dataset.layers" in editor["for_each"]


def test_the_three_layers_exist():
    assert {"raw", "staging", "marts"} <= set(load("main.tf")["locals"][0]["datasets"])
