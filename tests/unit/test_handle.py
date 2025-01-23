from decimal import Decimal

import pytest

from dbt.adapters.databricks.handle import clean_sql, translate_bindings


class TestHandle:
    @pytest.mark.parametrize(
        "bindings, expected", [(None, None), ([1], [1]), ([1, Decimal(0.73)], [1, 0.73])]
    )
    def test_translate_bindings(self, bindings, expected):
        assert translate_bindings(bindings) == expected

    @pytest.mark.parametrize(
        "sql, expected", [(" select 1; ", "select 1"), ("select 1", "select 1")]
    )
    def test_clean_sql(self, sql, expected):
        assert clean_sql(sql) == expected
