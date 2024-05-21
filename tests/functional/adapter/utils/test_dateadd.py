import pytest

import dbt.tests.adapter.utils.fixture_dateadd as fixtures
import tests.functional.adapter.utils.fixture_dateadd as fixture_overrides
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": fixture_overrides.seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": fixtures.models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                fixture_overrides.models__test_dateadd_sql, "dateadd"
            ),
        }
