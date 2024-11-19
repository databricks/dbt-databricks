from pathlib import Path

import pytest

from dbt.tests import util
from dbt.tests.adapter.simple_seed import seeds
from dbt.tests.adapter.simple_seed.test_seed import BaseSeedConfigFullRefreshOff
from dbt.tests.adapter.simple_seed.test_seed import BaseSeedCustomSchema
from dbt.tests.adapter.simple_seed.test_seed import BaseSeedParsing
from dbt.tests.adapter.simple_seed.test_seed import BaseSeedSpecificFormats
from dbt.tests.adapter.simple_seed.test_seed import BaseSeedWithEmptyDelimiter
from dbt.tests.adapter.simple_seed.test_seed import BaseSeedWithUniqueDelimiter
from dbt.tests.adapter.simple_seed.test_seed import BaseSeedWithWrongDelimiter
from dbt.tests.adapter.simple_seed.test_seed import BaseSimpleSeedEnabledViaConfig
from dbt.tests.adapter.simple_seed.test_seed import BaseSimpleSeedWithBOM
from dbt.tests.adapter.simple_seed.test_seed import BaseTestEmptySeed
from dbt.tests.adapter.simple_seed.test_seed import SeedTestBase
from tests.functional.adapter.fixtures import MaterializationV2Mixin
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


class TestBasicSeedTestsV2(DatabricksSetup, SeedTestBase, MaterializationV2Mixin):
    pass


class TestDatabricksSeedWithUniqueDelimiter(DatabricksSetup, BaseSeedWithUniqueDelimiter):
    pass


class TestDatabricksSeedWithUniqueDelimiterV2(
    DatabricksSetup, BaseSeedWithUniqueDelimiter, MaterializationV2Mixin
):
    pass


class TestDatabricksSeedWithWrongDelimiter(DatabricksSetup, BaseSeedWithWrongDelimiter):
    pass


class TestDatabricksSeedWithWrongDelimiterV2(
    DatabricksSetup, BaseSeedWithWrongDelimiter, MaterializationV2Mixin
):
    pass


class TestSeedConfigFullRefreshOff(DatabricksSetup, BaseSeedConfigFullRefreshOff):
    pass


class TestSeedConfigFullRefreshOffV2(
    DatabricksSetup, BaseSeedConfigFullRefreshOff, MaterializationV2Mixin
):
    pass


class TestSeedCustomSchema(DatabricksSetup, BaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(fixtures.seeds__expected_table_sql)
        project.run_sql(fixtures.seeds__expected_insert_sql)
        yield
        project.run_sql(f"drop schema if exists {project.test_schema}_custom_schema cascade")


class TestSeedCustomSchemaV2(TestSeedCustomSchema, MaterializationV2Mixin):
    pass


class TestDatabricksSeedWithEmptyDelimiter(DatabricksSetup, BaseSeedWithEmptyDelimiter):
    pass


class TestDatabricksSeedWithEmptyDelimiterV2(
    DatabricksSetup, BaseSeedWithEmptyDelimiter, MaterializationV2Mixin
):
    pass


class TestDatabricksEmptySeed(BaseTestEmptySeed):
    pass


class TestDatabricksEmptySeedV2(BaseTestEmptySeed, MaterializationV2Mixin):
    pass


class TestSimpleSeedEnabledViaConfig(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSimpleSeedEnabledViaConfigV2(BaseSimpleSeedEnabledViaConfig, MaterializationV2Mixin):
    pass


class TestSeedParsing(DatabricksSetup, BaseSeedParsing):
    pass


class TestSeedParsingV2(DatabricksSetup, BaseSeedParsing, MaterializationV2Mixin):
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


class TestSimpleSeedWithBOMV2(TestSimpleSeedWithBOM, MaterializationV2Mixin):
    pass


class TestSeedSpecificFormats(DatabricksSetup, BaseSeedSpecificFormats):
    @pytest.fixture(scope="class")
    def seeds(self):
        big_seed = "seed_id\n" + "\n".join(str(i) for i in range(1, 20001))

        yield {
            "big_seed.csv": big_seed,
            "seed_unicode.csv": seeds.seed__unicode_csv,
        }

    def test_simple_seed(self, project):
        results = util.run_dbt(["seed"])
        assert len(results) == 2


class TestSeedSpecificFormatsV2(TestSeedSpecificFormats, MaterializationV2Mixin):
    pass
