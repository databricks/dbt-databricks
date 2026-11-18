import pytest
from dbt.tests import util

from tests.functional.adapter.unit_testing import fixtures


class TestUnitTestTerminalSemicolon:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": fixtures.upstream_sql,
            "terminal_semicolon_model.sql": fixtures.terminal_semicolon_model_sql,
            "unit_tests.yml": fixtures.terminal_semicolon_unit_test_yml,
        }

    def test_unit_test_passes(self, project):
        util.run_dbt(["run", "--select", "upstream"])
        results = util.run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1
