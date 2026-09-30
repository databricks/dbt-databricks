import csv
import io
from decimal import Decimal
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

from tests.functional.adapter.fixtures import RerunSafeMixin
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
    def seeds(self):
        big_seed = "seed_id\n" + "\n".join(str(i) for i in range(1, 20001))

        yield {
            "big_seed.csv": big_seed,
            "seed_unicode.csv": seeds.seed__unicode_csv,
        }

    def test_simple_seed(self, project):
        results = util.run_dbt(["seed"])
        assert len(results) == 2


class TestSeedColumnTypes:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_column_types.csv": fixtures.seeds__column_types_csv,
            "schema.yml": fixtures.seeds__column_types_schema_yml,
        }

    def test_column_types_override(self, project):
        util.run_dbt(["seed"])
        relation = util.relation_from_name(project.adapter, "seed_column_types")
        # describe trails a blank row then "# ..." metadata sections; keep only the real columns.
        described = project.run_sql(f"describe {relation}", fetch="all")
        column_types = {
            col_name: data_type
            for col_name, data_type, *_ in described
            if col_name and not col_name.startswith("#")
        }
        assert column_types["rate"] == "double"
        assert column_types["amount"] == "decimal(10,2)"
        row_count = project.run_sql(f"select count(*) from {relation}", fetch="one")[0]
        assert row_count == 3


class TestSeedOntoView(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("seed_over_view",)

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_over_view.csv": fixtures.seeds__over_view_csv}

    def test_seed_onto_view_is_rejected(self, project):
        relation = util.relation_from_name(project.adapter, "seed_over_view")
        project.run_sql(f"create or replace view {relation} as select 1 as id")

        util.run_dbt(["seed"], expect_pass=False)

        # the pre-existing view must be left intact -- the seed must not replace it with a table
        with project.adapter.connection_named("_check_seed_over_view"):
            existing = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="seed_over_view",
            )
        assert existing is not None and existing.is_view


class TestSeedMaterializationV2FlagOn(RerunSafeMixin):
    """Seeds ignore use_materialization_v2; pin that the flag-on project gets the same lifecycle."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "seeds": {"post-hook": [fixtures.seeds__flag_on_post_hook_sql]},
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("seed_flag_on", "seed_flag_on_over_view")

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_flag_on.csv": fixtures.seeds__flag_on_initial_csv,
            "seed_flag_on_over_view.csv": fixtures.seeds__over_view_csv,
            "schema.yml": fixtures.seeds__flag_on_schema_yml,
        }

    @staticmethod
    def _expected_rows(csv_contents):
        rows = []
        for record in csv.DictReader(io.StringIO(csv_contents)):
            row = (
                int(record["id"]),
                record["name"],
                float(record["rate"]),
                Decimal(record["amount"]).quantize(Decimal("0.01")),
            )
            if "note" in record:
                row += (record["note"],)
            rows.append(row)
        return rows

    def _seed(self, project, csv_contents, step, *args):
        util.write_file(csv_contents, project.project_root, "seeds", "seed_flag_on.csv")
        vars_arg = f"{{seed_step: {step}}}"
        results = util.run_dbt(["seed", "--select", "seed_flag_on", "--vars", vars_arg, *args])
        assert len(results) == 1

    def _assert_seed_state(self, project, csv_contents, step):
        relation = util.relation_from_name(project.adapter, "seed_flag_on")
        expected_rows = self._expected_rows(csv_contents)
        column_list = "id, name, rate, amount" + (", note" if len(expected_rows[0]) == 5 else "")
        rows = project.run_sql(f"select {column_list} from {relation} order by id", fetch="all")
        assert [tuple(row) for row in rows] == expected_rows

        columns, table_comment = {}, None
        in_column_section = True
        for col_name, data_type, comment in project.run_sql(
            f"describe table extended {relation}", fetch="all"
        ):
            if not col_name or col_name.startswith("#"):
                in_column_section = False
            elif in_column_section:
                columns[col_name] = (data_type, comment)
            elif col_name == "Comment":
                table_comment = data_type
        assert columns["rate"][0] == "double"
        assert columns["amount"][0] == "decimal(10,2)"
        assert columns["id"][1] == "An id column"
        assert columns["name"][1] == "A name column"
        assert table_comment == "A seed description"

        properties = dict(project.run_sql(f"show tblproperties {relation}", fetch="all"))
        assert properties["dbt_seed_post_hook"] == str(step)

    def _seed_and_assert(self, project, csv_contents, step, *args):
        self._seed(project, csv_contents, step, *args)
        self._assert_seed_state(project, csv_contents, step)

    def test_seed_lifecycle(self, project):
        assert project.adapter.get_behavior_flag_no_warn("use_materialization_v2")

        self._seed_and_assert(project, fixtures.seeds__flag_on_initial_csv, 1)
        self._seed_and_assert(project, fixtures.seeds__flag_on_reseed_csv, 2)
        self._seed_and_assert(
            project, fixtures.seeds__flag_on_full_refresh_csv, 3, "--full-refresh"
        )

    def test_seed_onto_view_is_rejected(self, project):
        relation = util.relation_from_name(project.adapter, "seed_flag_on_over_view")
        project.run_sql(f"create or replace view {relation} as select 1 as id")

        util.run_dbt(
            ["seed", "--select", "seed_flag_on_over_view", "--vars", "{seed_step: 1}"],
            expect_pass=False,
        )

        with project.adapter.connection_named("_check_seed_flag_on_over_view"):
            existing = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="seed_flag_on_over_view",
            )
        assert existing is not None and existing.is_view
