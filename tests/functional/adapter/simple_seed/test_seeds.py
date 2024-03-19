from dbt.tests.adapter.simple_seed.test_seed import (
    SeedUniqueDelimiterTestBase,
    BaseTestEmptySeed,
    SeedTestBase,
)

from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.simple_seed import fixtures


# Can't pass the full-refresh flag test as Databricks does not have cascade support
class TestBasicSeedTests(SeedTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixtures.seeds__expected_table_sql)
        project.run_sql(fixtures.seeds__expected_insert_sql)

    def test_simple_seed(self, project):
        """Build models and observe that run truncates a seed and re-inserts rows"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)


class DatabricksSeedUniqueDelimiterTestBase(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(fixtures.seeds__expected_table_sql)
        project.run_sql(fixtures.seeds__expected_insert_sql)


class TestDatabricksSeedWithUniqueDelimiter(DatabricksSeedUniqueDelimiterTestBase):
    def test_seed_with_unique_delimiter(self, project):
        """Testing correct run of seeds with a unique delimiter (pipe in this case)"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)


class TestDatabricksSeedWithWrongDelimiter(DatabricksSeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ";"},
        }

    def test_seed_with_wrong_delimiter(self, project):
        """Testing failure of running dbt seed with a wrongly configured delimiter"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "syntax error" in seed_result.results[0].message.lower()


class TestDatabricksSeedWithEmptyDelimiter(DatabricksSeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": ""},
        }

    def test_seed_with_empty_delimiter(self, project):
        """Testing failure of running dbt seed with an empty configured delimiter value"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "compilation error" in seed_result.results[0].message.lower()


class TestDatabricksEmptySeed(BaseTestEmptySeed):
    pass
