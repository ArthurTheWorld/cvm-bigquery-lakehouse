import pytest

import bq_cleanup as bc


def test_drops_only_the_two_ephemeral_layers():
    assert bc.datasets_to_drop("dbt_ci_123_") == ["dbt_ci_123_staging", "dbt_ci_123_marts"]


@pytest.mark.parametrize("prefix", ["", "raw", "staging", "marts", "dbt_ci_", "dbt_ci__", "DBT_CI_1_", "dbt_ci_1", "dbt_ci_1_;drop",
                                    "dbt_ci_1_raw", "x_dbt_ci_1_", None])
def test_refuses_anything_that_could_hit_a_real_dataset(prefix):
    with pytest.raises(ValueError):
        bc.datasets_to_drop(prefix)


def test_real_datasets_are_never_in_the_drop_list():
    for dropped in bc.datasets_to_drop("dbt_ci_999_"):
        assert dropped not in {"raw", "staging", "marts"} and dropped.startswith("dbt_ci_")
