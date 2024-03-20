from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedWithUniqueDelimiter,
    BaseSeedWithWrongDelimiter,
    BaseSeedWithEmptyDelimiter,
    BaseTestEmptySeed,
    BaseSeedConfigFullRefreshOff,
    BaseSeedCustomSchema,
    SeedTestBase,
    BaseSimpleSeedEnabledViaConfig,
    BaseSeedParsing,
)

from dbt.tests.util import run_dbt
import pytest

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
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)


class TestDatabricksSeedWithUniqueDelimiter(DatabricksSetup, BaseSeedWithUniqueDelimiter):
    pass


class TestDatabricksSeedWithWrongDelimiter(DatabricksSetup, BaseSeedWithWrongDelimiter):
    pass


class TestSeedConfigFullRefreshOff(DatabricksSetup, BaseSeedConfigFullRefreshOff):
    pass


class TestSeedCustomSchema(DatabricksSetup, BaseSeedCustomSchema):
    pass


class TestDatabricksSeedWithEmptyDelimiter(DatabricksSetup, BaseSeedWithEmptyDelimiter):
    pass


class TestDatabricksEmptySeed(BaseTestEmptySeed):
    pass


class TestSimpleSeedEnabledViaConfig(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSeedParsing(DatabricksSetup, BaseSeedParsing):
    pass
