import pytest
from dbt.tests import util

from tests.functional.adapter.unit_testing import fixtures


class TestUnitTestHyphenatedNames:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my-upstream-model.sql": fixtures.my_upstream_model_sql,
            "my-incr-model.sql": fixtures.my_incr_model_sql,
            "schema.yml": fixtures.hyphenated_names_yml,
        }

    def test_unit_tests_with_hyphenated_inputs(self, project):
        results = util.run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 2
        assert all(result.status == "pass" for result in results)
