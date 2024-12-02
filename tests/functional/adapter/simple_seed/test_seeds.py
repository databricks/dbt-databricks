from pathlib import Path

import pytest

from dbt.tests import util
from dbt.tests.adapter.simple_seed import seeds
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedConfigFullRefreshOff,
    BaseSeedCustomSchema,
    BaseSeedParsing,
    BaseSeedSpecificFormats,
    BaseSeedWithEmptyDelimiter,
    BaseSeedWithUniqueDelimiter,
    BaseSeedWithWrongDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSimpleSeedWithBOM,
    BaseTestEmptySeed,
    SeedTestBase,
)
from tests.functional.adapter.simple_seed import fixtures


class DatabricksSetup:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(fixtures.seeds__expected_table_sql)
        project.run_sql(fixtures.seeds__expected_insert_sql)


# Can't pass the full-refresh flag test as Databricks does not have cascade support
class TestBasicSeedTests(DatabricksSetup, SeedTestBase):
    def test_simple_seed(self, project):
        """Build models and observe that run truncates a seed and re-inserts rows"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=util.run_dbt(["seed"]), project=project, exists=True
        )


class TestDatabricksSeedWithUniqueDelimiter(DatabricksSetup, BaseSeedWithUniqueDelimiter):
    pass


class TestDatabricksSeedWithWrongDelimiter(DatabricksSetup, BaseSeedWithWrongDelimiter):
    pass


class TestSeedConfigFullRefreshOff(DatabricksSetup, BaseSeedConfigFullRefreshOff):
    pass


class TestSeedCustomSchema(DatabricksSetup, BaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(fixtures.seeds__expected_table_sql)
        project.run_sql(fixtures.seeds__expected_insert_sql)
        yield
        project.run_sql(f"drop schema if exists {project.test_schema}_custom_schema cascade")


class TestDatabricksSeedWithEmptyDelimiter(DatabricksSetup, BaseSeedWithEmptyDelimiter):
    pass


class TestDatabricksEmptySeed(BaseTestEmptySeed):
    pass


class TestSimpleSeedEnabledViaConfig(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSeedParsing(DatabricksSetup, BaseSeedParsing):
    pass


class TestSimpleSeedWithBOM(BaseSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(fixtures.seeds__expected_table_sql)
        project.run_sql(fixtures.seeds__expected_insert_sql)
        util.copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


class TestSeedSpecificFormats(DatabricksSetup, BaseSeedSpecificFormats):
    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        big_seed_path = self._make_big_seed(test_data_dir)
        big_seed = util.read_file(big_seed_path)

        yield {
            "big_seed.csv": big_seed,
            "seed_unicode.csv": seeds.seed__unicode_csv,
        }
        util.rm_dir(test_data_dir)

    def test_simple_seed(self, project):
        results = util.run_dbt(["seed"])
        assert len(results) == 2
