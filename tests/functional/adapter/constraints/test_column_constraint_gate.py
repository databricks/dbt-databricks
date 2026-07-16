import pytest
from dbt.tests import util

from tests.functional.adapter.constraints import fixtures
from tests.functional.adapter.fixtures import (
    MaterializationV1Mixin,
    MaterializationV2Mixin,
    RerunSafeMixin,
)


def _constraint_rows(project, table_name, constraint_type):
    db = project.database.lower()
    sch = project.test_schema.lower()
    sql = f"""
        SELECT constraint_name
        FROM `{db}`.information_schema.table_constraints
        WHERE table_catalog = '{db}'
          AND table_schema = '{sch}'
          AND table_name = '{table_name.lower()}'
          AND constraint_type = '{constraint_type}'
    """
    return project.run_sql(sql, fetch="all")


def _pk_columns(project, table_name):
    db = project.database.lower()
    sch = project.test_schema.lower()
    sql = f"""
        SELECT kcu.column_name
        FROM `{db}`.information_schema.key_column_usage kcu
        WHERE kcu.table_catalog = '{db}'
          AND kcu.table_schema = '{sch}'
          AND kcu.table_name = '{table_name.lower()}'
          AND kcu.constraint_name IN (
              SELECT constraint_name
              FROM `{db}`.information_schema.table_constraints
              WHERE table_catalog = '{db}'
                AND table_schema = '{sch}'
                AND table_name = '{table_name.lower()}'
                AND constraint_type = 'PRIMARY KEY'
          )
        ORDER BY kcu.ordinal_position
    """
    return [row[0] for row in project.run_sql(sql, fetch="all")]


def _not_null_columns(project, table_name):
    db = project.database.lower()
    sch = project.test_schema.lower()
    sql = f"""
        SELECT column_name
        FROM `{db}`.information_schema.columns
        WHERE table_catalog = '{db}'
          AND table_schema = '{sch}'
          AND table_name = '{table_name.lower()}'
          AND is_nullable = 'NO'
    """
    return [row[0] for row in project.run_sql(sql, fetch="all")]


@pytest.mark.skip_profile("databricks_cluster")
class TestNoConstraintsWithoutContractEnforcement(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent_table.sql": fixtures.column_constraint_gate_parent_sql,
            "child_table.sql": fixtures.column_constraint_gate_child_sql,
            "schema.yml": fixtures.column_constraint_gate_child_schema_yml,
        }

    def test_neither_column_nor_model_constraints_are_applied(self, project):
        util.run_dbt(["run"])

        pk_rows = _constraint_rows(project, "child_table", "PRIMARY KEY")
        assert len(pk_rows) == 0, (
            f"Expected no PRIMARY KEY on child_table without contract.enforced, found {pk_rows}"
        )

        fk_rows = _constraint_rows(project, "child_table", "FOREIGN KEY")
        assert len(fk_rows) == 0, (
            f"Expected no FOREIGN KEY on child_table without contract.enforced "
            f"(column-level FK must be gated), found {fk_rows}"
        )

        not_null_cols = _not_null_columns(project, "child_table")
        assert not_null_cols == [], (
            f"Expected no NOT NULL columns on child_table without contract.enforced "
            f"(column-level not_null must be gated), found {not_null_cols}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestConstraintsApplyWithContractEnforced(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent_table.sql": fixtures.column_constraint_gate_parent_sql,
            "child_with_contract.sql": fixtures.column_constraint_gate_child_with_contract_sql,
            "schema.yml": fixtures.column_constraint_gate_child_with_contract_schema_yml,
        }

    def test_constraints_apply_and_survive_rerun(self, project):
        util.run_dbt(["run"])

        pk_rows = _constraint_rows(project, "child_with_contract", "PRIMARY KEY")
        assert len(pk_rows) == 1, (
            f"Expected one PRIMARY KEY on child_with_contract after first run, found {pk_rows}"
        )

        pk_cols = _pk_columns(project, "child_with_contract")
        assert pk_cols == ["hashkey", "load_timestamp"], (
            f"Expected PK columns ['hashkey', 'load_timestamp'], got {pk_cols}"
        )

        fk_rows = _constraint_rows(project, "child_with_contract", "FOREIGN KEY")
        assert len(fk_rows) == 1, (
            f"Expected one FOREIGN KEY on child_with_contract after first run, found {fk_rows}"
        )

        util.run_dbt(["run", "--select", "child_with_contract"])

        pk_rows_after = _constraint_rows(project, "child_with_contract", "PRIMARY KEY")
        assert len(pk_rows_after) == 1, (
            f"Expected PRIMARY KEY to survive the second run, found {pk_rows_after}"
        )
        pk_cols_after = _pk_columns(project, "child_with_contract")
        assert pk_cols_after == ["hashkey", "load_timestamp"], (
            f"Expected PK columns preserved after re-run, got {pk_cols_after}"
        )
        fk_rows_after = _constraint_rows(project, "child_with_contract", "FOREIGN KEY")
        assert len(fk_rows_after) == 1, (
            f"Expected FOREIGN KEY to survive the second run, found {fk_rows_after}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestV1ContractConstraintsApplied(RerunSafeMixin, MaterializationV1Mixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        # child holds the FK to parent_table, so drop it first.
        return ("v1_contract_child", "parent_table")

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parent_table.sql": fixtures.column_constraint_gate_parent_sql,
            "v1_contract_child.sql": fixtures.v1_contract_child_table_sql,
            "schema.yml": fixtures.v1_contract_child_table_schema_yml,
        }

    def _assert_all_constraints_present(self, project):
        pk_rows = _constraint_rows(project, "v1_contract_child", "PRIMARY KEY")
        assert len(pk_rows) == 1, f"Expected one PRIMARY KEY, found {pk_rows}"
        assert _pk_columns(project, "v1_contract_child") == ["id"]

        fk_rows = _constraint_rows(project, "v1_contract_child", "FOREIGN KEY")
        assert len(fk_rows) == 1, f"Expected one FOREIGN KEY, found {fk_rows}"

        not_null_cols = set(_not_null_columns(project, "v1_contract_child"))
        assert {"id", "name", "parent_id"}.issubset(not_null_cols), (
            f"Expected id/name/parent_id NOT NULL, got {sorted(not_null_cols)}"
        )

    def test_v1_contract_constraints_in_information_schema(self, project):
        util.run_dbt(["run"])
        self._assert_all_constraints_present(project)

        # V1 tables recreate on every run, so the constraints must be re-applied.
        util.run_dbt(["run"])
        self._assert_all_constraints_present(project)
