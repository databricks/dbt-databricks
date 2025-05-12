from typing import Optional

import pytest

from dbt.adapters.databricks import catalogs, constants
from tests.unit.macros.base import MacroTestBase


def unity_relation(
    table_format: Optional[str] = None,
    file_format: Optional[str] = None,
    location_root: Optional[str] = None,
    location_path: Optional[str] = None,
) -> catalogs.DatabricksCatalogRelation:
    catalog_integration = constants.DEFAULT_UNITY_CATALOG
    return catalogs.DatabricksCatalogRelation(
        catalog_type=catalog_integration.catalog_type,
        catalog_name=catalog_integration.catalog_name,
        table_format=table_format or catalog_integration.table_format,
        file_format=file_format or catalog_integration.adapter_properties.get("file_format"),
        external_volume=location_root or catalog_integration.external_volume,
        location_path=location_path,
    )


class TestCreateTableAs(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/table", "macros/relations", "macros"]

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return ["file_format.sql", "tblproperties.sql", "location.sql", "liquid_clustering.sql"]

    @pytest.fixture
    def context(self, template) -> dict:
        """
        Access to the context used to render the template.
        Modification of the context will work for mocking adapter calls, but may not work for
        mocking macros.
        If you need to mock a macro, see the use of is_incremental in default_context.
        """
        template.globals["adapter"].update_tblproperties_for_iceberg.return_value = {}
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
        template_bundle.context[
            "adapter"
        ].update_tblproperties_for_iceberg.return_value = catalog_relation.iceberg_table_properties  # type: ignore
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

        assert sql == expected

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
        template_bundle.context["adapter"].update_tblproperties_for_iceberg.return_value = {
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

        assert sql == expected

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
        template_bundle.context["adapter"].update_tblproperties_for_iceberg.return_value = {
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
