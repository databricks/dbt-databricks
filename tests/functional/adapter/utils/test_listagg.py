# requires modification
import dbt.tests.adapter.utils.fixture_listagg as fixtures
import pytest
from dbt.tests.adapter.utils.test_listagg import BaseListagg

import tests.functional.adapter.utils.fixture_listagg as fixture_overrides


# SparkSQL does not support 'order by' for its 'listagg' equivalent
# the argument is ignored, so let's ignore those fields when checking equivalency
class TestListagg(BaseListagg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": fixtures.models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                fixture_overrides.models__test_listagg_no_order_by_sql, "listagg"
            ),
        }
