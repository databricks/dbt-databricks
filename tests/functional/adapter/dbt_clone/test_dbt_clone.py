import pytest
from dbt.tests import util
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClone, BaseClonePossible
from dbt.tests.util import run_dbt

from tests.functional.adapter.dbt_clone import fixtures
from tests.functional.adapter.fixtures import (
    MaterializationV2Mixin,
    RequiresDescribeAsJsonCapabilityMixin,
    RerunSafeMixin,
)


class CleanupMixin:
    @pytest.fixture(scope="class", autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)


class TestClonePossible(BaseClonePossible, CleanupMixin):
    pass


@pytest.mark.skip("Skip until tests fixed upstream in 1.8.0 final")
class TestCloneSameTargetAndState(BaseClone, CleanupMixin):
    def test_clone_same_target_and_state(self, project, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)

        clone_args = [
            "clone",
            "--state",
            "target",
        ]

        results, output = util.run_dbt_and_capture(clone_args, expect_pass=False)
        assert "Warning: The state and target directories are the same: 'target'" in output


class TestClonePersistDocs(BaseClone):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": fixtures.table_model_sql,
            "view_model.sql": fixtures.view_model_sql,
            "schema.yml": fixtures.comment_schema_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_persist_docs(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        results = run_dbt(["run"])
        assert len(results) == 2
        self.copy_state(project.project_root)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)

        results = project.run_sql(
            f"describe extended {project.database}.{other_schema}.table_model",
            fetch="all",
        )
        table_comment = next(row[1] for row in results if row[0].strip() == "Comment")
        assert table_comment == "This is a table model"

        results = project.run_sql(
            f"describe extended {project.database}.{other_schema}.view_model",
            fetch="all",
        )
        view_comment = next(row[1] for row in results if row[0].strip() == "Comment")
        assert view_comment == "This is a view model"


def _delta_history(project, relation):
    """Return (latest_version, operations, versions) from DESCRIBE HISTORY."""
    history = project.run_sql(f"describe history {relation}", fetch="all")
    versions = {row[0] for row in history}
    operations = {row[4] for row in history}
    return max(versions), operations, versions


class TestCloneShallowClone(BaseClone, CleanupMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_model.sql": fixtures.table_model_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    def test_shallow_clone(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        run_dbt(["run"])
        self.copy_state(project.project_root)

        clone_args = ["clone", "--state", "state", "--target", "otherschema"]
        run_dbt(clone_args)

        cloned = f"{project.database}.{other_schema}.table_model"

        # the table branch materializes via CREATE OR REPLACE ... SHALLOW CLONE,
        # which Delta records as a CLONE operation rather than a full rewrite
        version, operations, versions_before = _delta_history(project, cloned)
        assert "CLONE" in operations, f"clone history operations: {operations}"

        # an existing target is left untouched without --full-refresh
        run_dbt(clone_args)
        noop_version, _, versions_before = _delta_history(project, cloned)
        assert noop_version == version

        # --full-refresh re-clones in place, adding a fresh CLONE version
        run_dbt([*clone_args, "--full-refresh"])
        refreshed_version, refreshed_operations, refreshed_versions = _delta_history(
            project, cloned
        )
        assert refreshed_version > version
        assert versions_before.issubset(refreshed_versions), (
            f"in-place full-refresh must keep prior history versions; "
            f"lost {versions_before - refreshed_versions} from {refreshed_versions}"
        )
        assert "CLONE" in refreshed_operations, (
            f"full-refresh history operations: {refreshed_operations}"
        )


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestCloneHmsRelationMatrix(BaseClone, CleanupMixin):
    """HMS listing omits Databricks table types, so full-refresh clone must replace
    unknown-type tables in place (preserving Delta history) rather than drop-first.

    UC profiles skip: a known managed target is intentionally dropped there, which
    resets history to version 0 and would fail the survival assertions below.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hms_clone_new_model.sql": fixtures.table_model_sql,
            "hms_clone_regular_model.sql": fixtures.table_model_sql,
            "hms_clone_shallow_model.sql": fixtures.table_model_sql,
            "hms_clone_view_model.sql": fixtures.table_model_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    def _target(self, project, schema, identifier):
        return f"`{project.database}`.`{schema}`.`{identifier}`"

    def test_clone_full_refresh_over_hms_relation_shapes(
        self, project, unique_schema, other_schema
    ):
        project.create_test_schema(other_schema)
        run_dbt(["run"])
        self.copy_state(project.project_root)

        regular = self._target(project, other_schema, "hms_clone_regular_model")
        shallow = self._target(project, other_schema, "hms_clone_shallow_model")
        view = self._target(project, other_schema, "hms_clone_view_model")

        run_dbt(["run", "--target", "otherschema", "-s", "hms_clone_regular_model"])
        regular_before, _, regular_versions_before = _delta_history(project, regular)

        run_dbt(
            [
                "clone",
                "--state",
                "state",
                "--target",
                "otherschema",
                "-s",
                "hms_clone_shallow_model",
            ]
        )
        shallow_before, shallow_before_operations, shallow_versions_before = _delta_history(
            project, shallow
        )
        assert "CLONE" in shallow_before_operations

        project.run_sql(f"create or replace view {view} as select 2 as id")

        run_dbt(["clone", "--state", "state", "--target", "otherschema", "--full-refresh"])

        histories = {
            identifier: _delta_history(project, self._target(project, other_schema, identifier))
            for identifier in (
                "hms_clone_new_model",
                "hms_clone_regular_model",
                "hms_clone_shallow_model",
                "hms_clone_view_model",
            )
        }
        for identifier, (_, operations, _) in histories.items():
            assert "CLONE" in operations, f"{identifier} history operations: {operations}"

        regular_latest, _, regular_versions = histories["hms_clone_regular_model"]
        shallow_latest, _, shallow_versions = histories["hms_clone_shallow_model"]
        assert regular_latest > regular_before
        assert regular_versions_before.issubset(regular_versions), (
            f"HMS in-place replace must keep prior history; "
            f"lost {regular_versions_before - regular_versions}"
        )
        assert shallow_latest > shallow_before
        assert shallow_versions_before.issubset(shallow_versions), (
            f"HMS in-place replace must keep prior history; "
            f"lost {shallow_versions_before - shallow_versions}"
        )
        assert project.run_sql(f"select id from {view}", fetch="all")[0][0] == 1


def _table_type(project, schema, identifier):
    rows = project.run_sql(
        f"""
        select table_type from {project.database}.information_schema.tables
        where table_schema = '{schema}' and table_name = '{identifier}'
        """,
        fetch="all",
    )
    return rows[0][0] if rows else None


@pytest.mark.skip_profile("databricks_cluster")
class TestCloneOverExistingManagedTable(BaseClone, CleanupMixin):
    """`dbt clone --full-refresh` must replace an existing managed table with a shallow clone,
    rather than failing with INVALID_PARAMETER_VALUE.UPDATE_TABLE_TYPE."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"table_model.sql": fixtures.table_model_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    def test_clone_replaces_managed_table(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        # Build a real managed table at the clone target first.
        run_dbt(["run", "--target", "otherschema"])
        assert _table_type(project, other_schema, "table_model") == "MANAGED"

        # State for the clone comes from the default-target build.
        run_dbt(["run"])
        self.copy_state(project.project_root)

        clone_args = ["clone", "--state", "state", "--target", "otherschema", "--full-refresh"]
        run_dbt(clone_args)

        # The managed table is dropped and replaced by a shallow clone.
        assert _table_type(project, other_schema, "table_model") == "MANAGED_SHALLOW_CLONE"


@pytest.mark.skip_profile("databricks_cluster")
class TestRebuildOverShallowClone(BaseClone, CleanupMixin):
    """Rebuilding a table/incremental model with auto_liquid_cluster over an existing shallow
    clone must drop the clone and produce a managed table, rather than failing with
    CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "auto_cluster_table_model.sql": fixtures.auto_cluster_table_model_sql,
            "auto_cluster_incremental_model.sql": fixtures.auto_cluster_incremental_model_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.mark.parametrize(
        "identifier",
        ["auto_cluster_table_model", "auto_cluster_incremental_model"],
    )
    def test_rebuild_over_shallow_clone(self, project, unique_schema, other_schema, identifier):
        project.create_test_schema(other_schema)
        run_dbt(["run"])
        self.copy_state(project.project_root)

        # Clone into the other schema so the target starts life as a shallow clone.
        run_dbt(["clone", "--state", "state", "--target", "otherschema"])
        assert _table_type(project, other_schema, identifier) == "MANAGED_SHALLOW_CLONE"

        # Rebuilding over the clone must drop it and produce a managed table with correct data.
        run_dbt(["run", "--target", "otherschema", "--full-refresh", "-s", identifier])
        assert _table_type(project, other_schema, identifier) == "MANAGED"

        rows = project.run_sql(
            f"select id from {project.database}.{other_schema}.{identifier}",
            fetch="all",
        )
        assert [row[0] for row in rows] == [1]


class TestCloneViewWithChangedContract(RerunSafeMixin, BaseClone, CleanupMixin):
    """A view whose enforced contract changed since the deferred state still clones (#1349)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": fixtures.contract_view_model_sql,
            "schema.yml": fixtures.contract_view_schema_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ["view_model"]

    @pytest.fixture(autouse=True)
    def reset_clone_target(self, project, other_schema):
        project.create_test_schema(other_schema)
        project.run_sql(f"drop view if exists `{project.database}`.`{other_schema}`.`view_model`")

    def test_clone_view_with_changed_contract(self, project, unique_schema, other_schema):
        run_dbt(["run"])
        self.copy_state(project.project_root)

        util.write_file(
            fixtures.contract_view_model_renamed_sql,
            project.project_root,
            "models",
            "view_model.sql",
        )
        util.write_file(
            fixtures.contract_view_renamed_schema_yml, project.project_root, "models", "schema.yml"
        )

        run_dbt(["clone", "--state", "state", "--target", "otherschema"])

        rows = project.run_sql(
            f"select id, name from {project.database}.{other_schema}.view_model", fetch="all"
        )
        assert [tuple(row) for row in rows] == [(1, "a")]


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneRowFilteredMaterializedView(BaseClone, CleanupMixin):
    """A materialized view with a row filter clones to a view that reads through the filter."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "governed_model.sql": fixtures.row_filter_mv_sql,
            "schema.yml": fixtures.row_filter_schema_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    policy_function = "region_a_filter(region string) returns boolean return region = 'a'"
    select_columns = "region, id"
    expected_rows: list[tuple] = [("a", 1)]

    def build_prod(self, project):
        run_dbt(["run"])

    def test_clone_governed_model(self, project, other_schema):
        project.run_sql(
            f"create or replace function {project.database}.{project.test_schema}."
            f"{self.policy_function}"
        )
        project.create_test_schema(other_schema)
        self.build_prod(project)
        self.copy_state(project.project_root)

        run_dbt(["clone", "--state", "state", "--target", "otherschema"])

        assert _table_type(project, other_schema, "governed_model") == "VIEW"
        rows = project.run_sql(
            f"select {self.select_columns} from {project.database}.{other_schema}.governed_model",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == self.expected_rows


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneRowFilteredStreamingTable(TestCloneRowFilteredMaterializedView):
    """A streaming table with a row filter clones to a view that reads through the filter."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "governed_model.sql": fixtures.row_filter_st_sql,
            "schema.yml": fixtures.row_filter_schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"row_filter_seed.csv": fixtures.row_filter_seed_csv}

    def build_prod(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneColumnMaskedStreamingTable(TestCloneRowFilteredStreamingTable):
    """A streaming table with a column mask clones to a view that reads through the mask."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "governed_model.sql": fixtures.column_mask_st_sql,
            "schema.yml": fixtures.column_mask_schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"column_mask_seed.csv": fixtures.column_mask_seed_csv}

    policy_function = "password_mask(password string) returns string return '*****'"
    select_columns = "id, password"
    expected_rows = [("abc123", "*****")]


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneRowFilteredTable(TestCloneRowFilteredMaterializedView):
    """A table with a row filter can't be shallow-cloned, so it clones to a view that reads
    through the filter."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "governed_model.sql": fixtures.row_filter_table_sql,
            "schema.yml": fixtures.row_filter_schema_yml,
        }


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneRowFilterAppliedOutsideDbt(TestCloneRowFilteredTable):
    """The clone follows the deferred table's policies, not the model config."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"governed_model.sql": fixtures.row_filter_table_sql}

    def build_prod(self, project):
        run_dbt(["run"])
        schema = f"{project.database}.{project.test_schema}"
        project.run_sql(
            f"alter table {schema}.governed_model "
            f"set row filter {schema}.region_a_filter on (region)"
        )


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneColumnMaskedTable(MaterializationV2Mixin, TestCloneRowFilteredTable):
    """A table with a column mask clones to a view that reads through the mask.

    V1 tables don't apply column masks at create, so prod is built with V2.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "governed_model.sql": fixtures.column_mask_table_sql,
            "schema.yml": fixtures.column_mask_schema_yml,
        }

    policy_function = "password_mask(password string) returns string return '*****'"
    select_columns = "id, password"
    expected_rows = [("abc123", "*****")]


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneRowFilterAppliedOutsideDbtDescribeAsJson(
    RequiresDescribeAsJsonCapabilityMixin, TestCloneRowFilterAppliedOutsideDbt
):
    """The DESCRIBE TABLE EXTENDED AS JSON lookup also finds a row filter."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_describe_as_json_for_relation_metadata": True}}


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestCloneColumnMaskedTableDescribeAsJson(
    RequiresDescribeAsJsonCapabilityMixin, TestCloneColumnMaskedTable
):
    """The DESCRIBE TABLE EXTENDED AS JSON lookup also finds a column mask."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "use_materialization_v2": True,
                "use_describe_as_json_for_relation_metadata": True,
            }
        }
