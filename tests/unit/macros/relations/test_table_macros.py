from unittest.mock import Mock

import pytest
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.constraints import (
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    synthesize_constraint_name,
)
from dbt.adapters.databricks.relation import DatabricksRelation
from tests.unit.macros.base import MacroTestBase
from tests.unit.utils import unity_relation


class TestCreateTableAs(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return [
            "macros/relations/table",
            "macros/relations",
            "macros",
            "macros/relations/components",
        ]

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return [
            "file_format.sql",
            "tblproperties.sql",
            "location.sql",
            "liquid_clustering.sql",
            "row_filter.sql",
        ]

    @pytest.fixture
    def context(self, template) -> dict:
        """
        Access to the context used to render the template.
        Modification of the context will work for mocking adapter calls, but may not work for
        mocking macros.
        If you need to mock a macro, see the use of is_incremental in default_context.
        """
        template.globals["adapter"].is_uniform.return_value = False
        template.globals["adapter"].update_tblproperties_for_uniform_iceberg.return_value = {}
        return template.globals

    def render_create_table_as(self, template_bundle, temporary=False, sql="select 1"):
        external_path = f"/mnt/root/{template_bundle.relation.identifier}"
        adapter_mock = template_bundle.template.globals["adapter"]
        adapter_mock.compute_external_path.return_value = external_path
        return self.run_macro(
            template_bundle.template,
            "databricks__create_table_as",
            temporary,
            template_bundle.relation,
            sql,
        )

    def test_macros_create_table_as(self, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        sql = self.render_create_table_as(template_bundle)
        assert (
            sql == f"create or replace table {template_bundle.relation.render()}"
            " using delta as select 1"
        )

    def test_macros_create_table_as_with_iceberg(self, template_bundle):
        catalog_relation = unity_relation(table_format=constants.ICEBERG_TABLE_FORMAT)
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation
        template_bundle.context["adapter"].is_uniform.return_value = True
        template_bundle.context["adapter"].update_tblproperties_for_uniform_iceberg.return_value = (
            catalog_relation.iceberg_table_properties  # type: ignore
        )
        template_bundle.context["adapter"].behavior.use_managed_iceberg = False
        sql = self.render_create_table_as(template_bundle)
        assert sql == self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} using delta"
            " tblproperties ('delta.enableIcebergCompatV2' = 'true' , "
            "'delta.universalFormat.enabledFormats' = 'iceberg') as select 1"
        )

    @pytest.mark.parametrize("format", [constants.PARQUET_FILE_FORMAT, constants.HUDI_FILE_FORMAT])
    def test_macros_create_table_as_file_format(self, format, config, template_bundle):
        catalog_relation = unity_relation(
            file_format=format,
            location_root="/mnt/root",
            location_path=template_bundle.relation.identifier,
        )
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation
        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create table {template_bundle.relation.render()} using {format} location "
            f"'/mnt/root/{template_bundle.relation.identifier}' as select 1"
        )
        assert sql == expected

    def test_macros_create_table_as_options(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["options"] = {"compression": "gzip"}
        sql = self.render_create_table_as(template_bundle)
        expected = self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} "
            'using delta options (compression "gzip" ) as select 1'
        )

        assert sql == expected

    def test_macros_create_table_as_hudi_unique_key(self, config, template_bundle):
        catalog_relation = unity_relation(
            file_format=constants.HUDI_FILE_FORMAT,
            location_root="/mnt/root",
            location_path=template_bundle.relation.identifier,
        )
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation

        config["unique_key"] = "id"
        sql = self.render_create_table_as(template_bundle, sql="select 1 as id")

        expected = self.clean_sql(
            f'create table {template_bundle.relation.render()} using hudi options (primaryKey "id")'
            f" location '/mnt/root/{template_bundle.relation.identifier}'"
            " as select 1 as id"
        )

        assert sql == expected

    def test_macros_create_table_as_hudi_unique_key_primary_key_match(
        self, config, template_bundle
    ):
        catalog_relation = unity_relation(
            file_format=constants.HUDI_FILE_FORMAT,
            location_root="/mnt/root",
            location_path=template_bundle.relation.identifier,
        )
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation
        config["unique_key"] = "id"
        config["options"] = {"primaryKey": "id"}
        sql = self.render_create_table_as(template_bundle, sql="select 1 as id")

        expected = self.clean_sql(
            f'create table {template_bundle.relation.render()} using hudi options (primaryKey "id")'
            f" location '/mnt/root/{template_bundle.relation.identifier}'"
            " as select 1 as id"
        )
        assert sql == expected

    def test_macros_create_table_as_hudi_unique_key_primary_key_mismatch(
        self, config, template_bundle
    ):
        catalog_relation = unity_relation(file_format=constants.HUDI_FILE_FORMAT)
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation
        config["unique_key"] = "uuid"
        config["options"] = {"primaryKey": "id"}
        sql = self.render_create_table_as(template_bundle, sql="select 1 as id, 2 as uuid")
        assert "mock.raise_compiler_error()" in sql

    def test_macros_create_table_as_partition(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["partition_by"] = "partition_1"
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation.render()} using delta"
            " partitioned by (partition_1) as select 1"
        )
        assert sql == expected

    def test_macros_create_table_as_partitions(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["partition_by"] = ["partition_1", "partition_2"]
        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create or replace table {template_bundle.relation.render()} "
            "using delta partitioned by (partition_1,partition_2) as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_cluster(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["clustered_by"] = "cluster_1"
        config["buckets"] = "1"
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation.render()} "
            "using delta clustered by (cluster_1) into 1 buckets as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_clusters(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["clustered_by"] = ["cluster_1", "cluster_2"]
        config["buckets"] = "1"
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation.render()} "
            "using delta clustered by (cluster_1,cluster_2) into 1 buckets as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_liquid_cluster(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["liquid_clustered_by"] = "cluster_1"
        sql = self.render_create_table_as(template_bundle)
        expected = self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} using"
            " delta CLUSTER BY (cluster_1) as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_liquid_clusters(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["liquid_clustered_by"] = ["cluster_1", "cluster_2"]
        config["buckets"] = "1"
        sql = self.render_create_table_as(template_bundle)
        expected = self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} "
            "using delta CLUSTER BY (cluster_1, cluster_2) as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_liquid_cluster_auto(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["auto_liquid_cluster"] = True
        sql = self.render_create_table_as(template_bundle)
        expected = self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} using"
            " delta CLUSTER BY AUTO as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_comment(self, config, template_bundle):
        template_bundle.context["adapter"].build_catalog_relation.return_value = unity_relation()
        config["persist_docs"] = {"relation": True}
        template_bundle.context["model"].description = "Description Test"

        sql = self.render_create_table_as(template_bundle)

        expected = self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} "
            "using delta comment 'Description Test' as select 1"
        )

        assert expected == sql

    def test_macros_create_table_as_all_delta(self, config, template_bundle):
        catalog_relation = unity_relation(
            file_format=constants.DELTA_FILE_FORMAT,
            location_root="/mnt/root",
            location_path=template_bundle.relation.identifier,
        )
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation

        config["partition_by"] = ["partition_1", "partition_2"]
        config["liquid_clustered_by"] = ["cluster_1", "cluster_2"]
        config["clustered_by"] = ["cluster_1", "cluster_2"]
        config["buckets"] = "1"
        config["persist_docs"] = {"relation": True}
        template_bundle.context["adapter"].is_uniform.return_value = True
        template_bundle.context["adapter"].update_tblproperties_for_uniform_iceberg.return_value = {
            "delta.appendOnly": "true"
        }
        template_bundle.context["model"].description = "Description Test"

        sql = self.render_create_table_as(template_bundle)

        expected = self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} "
            "using delta "
            "partitioned by (partition_1,partition_2) "
            "CLUSTER BY (cluster_1, cluster_2) "
            "clustered by (cluster_1,cluster_2) into 1 buckets "
            "location '/mnt/root/some_table' "
            "comment 'Description Test' "
            "tblproperties ('delta.appendOnly' = 'true' ) "
            "as select 1"
        )

        assert expected == sql

    def test_macros_create_table_as_all_hudi(self, config, template_bundle):
        catalog_relation = unity_relation(
            file_format=constants.HUDI_FILE_FORMAT,
            location_root="/mnt/root",
            location_path=template_bundle.relation.identifier,
        )
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation

        config["partition_by"] = ["partition_1", "partition_2"]
        config["clustered_by"] = ["cluster_1", "cluster_2"]
        config["buckets"] = "1"
        config["persist_docs"] = {"relation": True}
        template_bundle.context["adapter"].is_uniform.return_value = True
        template_bundle.context["adapter"].update_tblproperties_for_uniform_iceberg.return_value = {
            "delta.appendOnly": "true"
        }
        template_bundle.context["model"].description = "Description Test"

        sql = self.render_create_table_as(template_bundle)

        expected = self.clean_sql(
            f"create table {template_bundle.relation.render()} "
            "using hudi "
            "partitioned by (partition_1,partition_2) "
            "clustered by (cluster_1,cluster_2) into 1 buckets "
            "location '/mnt/root/some_table' "
            "comment 'Description Test' "
            "tblproperties ('delta.appendOnly' = 'true' ) "
            "as select 1"
        )
        assert sql == expected

    def test_macros_create_table_as_managed_iceberg(self, config, template_bundle):
        """Test that USING ICEBERG is generated when managed Iceberg flag is enabled"""
        catalog_relation = unity_relation(table_format=constants.ICEBERG_TABLE_FORMAT)
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation
        template_bundle.context["adapter"].behavior.use_managed_iceberg = True

        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create or replace table {template_bundle.relation.render()} using iceberg as select 1"
        )
        assert sql == expected

    def test_macros_create_table_as_uniform_iceberg(self, config, template_bundle):
        """Test that USING DELTA is still used when managed Iceberg flag is disabled (default)"""
        catalog_relation = unity_relation(table_format=constants.ICEBERG_TABLE_FORMAT)
        template_bundle.context["adapter"].build_catalog_relation.return_value = catalog_relation
        template_bundle.context["adapter"].behavior.use_managed_iceberg = False
        # Mock the UniForm properties return
        template_bundle.context["adapter"].is_uniform.return_value = True
        template_bundle.context["adapter"].update_tblproperties_for_uniform_iceberg.return_value = {
            "delta.enableIcebergCompatV2": "true",
            "delta.universalFormat.enabledFormats": "iceberg",
        }

        sql = self.render_create_table_as(template_bundle)
        expected = self.clean_sql(
            f"create or replace table {template_bundle.relation.render()} using delta "
            "tblproperties ('delta.enableIcebergCompatV2' = 'true' , "
            "'delta.universalFormat.enabledFormats' = 'iceberg') as select 1"
        )
        assert sql == expected


class TestFileFormatClause(MacroTestBase):
    """Regression tests for `file_format_clause` short-circuit behavior.

    For non-Iceberg models, the macro must not read `adapter.behavior.use_managed_iceberg`,
    since that access fires a BehaviorChangeEvent deprecation warning on every run.
    """

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "file_format.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    def test_file_format_clause_does_not_access_flag_for_non_iceberg(self, template_bundle):
        """Non-Iceberg relations must short-circuit before reading the behavior flag."""

        class ErrorOnUseManagedIcebergAccess:
            @property
            def use_managed_iceberg(self):
                raise AssertionError(
                    "use_managed_iceberg must not be accessed when table_format != 'iceberg'"
                )

        template_bundle.context["adapter"].behavior = ErrorOnUseManagedIcebergAccess()
        catalog_relation = unity_relation(
            table_format=constants.DEFAULT_TABLE_FORMAT,
            file_format=constants.DELTA_FILE_FORMAT,
        )

        sql = self.run_macro(template_bundle.template, "file_format_clause", catalog_relation)

        assert sql == "using delta"


def _unnamed_pk():
    return PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"])


def _unnamed_self_fk():
    return ForeignKeyConstraint(
        type=ConstraintType.foreign_key, columns=["parent_id"], to="p", to_columns=["id"]
    )


def _named_fk():
    return ForeignKeyConstraint(
        type=ConstraintType.foreign_key,
        name="fk_named",
        columns=["other_id"],
        to="o",
        to_columns=["id"],
    )


class TestSafeRelationReplace(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/table", "macros/relations", "macros"]

    def test_staged_keys_renamed_to_final_names_after_backup_dropped(self, template_bundle):
        context = template_bundle.context
        statements = []

        def statement(name, caller):
            statements.append(self.clean_sql(caller()))
            return ""

        context["statement"] = statement
        key_constraints = [_unnamed_pk(), _unnamed_self_fk(), _named_fk()]
        context["create_table_at"] = Mock(return_value="")
        context["get_model_key_constraints"] = Mock(return_value=key_constraints)
        context["get_drop_backup_sql"] = Mock(return_value="drop backup")
        for macro in ("create_backup", "make_backup_relation", "drop_relation_if_exists"):
            context[macro] = Mock(return_value="")
        context["this"] = DatabricksRelation.create(
            database="c", schema="s", identifier="My_Model", type="table"
        )
        staging = DatabricksRelation.create(
            database="c", schema="s", identifier="My_Model__dbt_stg", type="table", is_staging=True
        )
        intermediate = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "safe_relation_replace",
            template_bundle.relation,
            staging,
            intermediate,
            "select 1",
        )

        context["create_table_at"].assert_called_once_with(staging, intermediate, "select 1")
        stg_pk = synthesize_constraint_name(_unnamed_pk(), "My_Model__dbt_stg")
        stg_fk = synthesize_constraint_name(_unnamed_self_fk(), "My_Model__dbt_stg")
        stg_named = synthesize_constraint_name(_named_fk(), "My_Model__dbt_stg")
        pk = synthesize_constraint_name(_unnamed_pk(), "My_Model")
        fk = synthesize_constraint_name(_unnamed_self_fk(), "My_Model")
        target = "alter table `c`.`s`.`my_model`"
        assert statements == [
            "drop backup",
            f"{target} drop constraint {stg_fk}",
            f"{target} drop constraint {stg_named}",
            f"{target} drop constraint {stg_pk}",
            f"{target} add constraint {pk} primary key (id)",
            f"{target} add constraint {fk} foreign key (parent_id) references p (id)",
            f"{target} add constraint fk_named foreign key (other_id) references o (id)",
        ]
