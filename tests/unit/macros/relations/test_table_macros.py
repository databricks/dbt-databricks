import pytest

from tests.unit.macros.base import MacroTestBase


class TestCreateTableAs(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/table", "macros/relations", "macros"]

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return ["file_format.sql", "tblproperties.sql", "location.sql"]

    def render_create_table_as(self, template_bundle, temporary=False, sql="select 1"):
        return self.run_macro(
            template_bundle.template,
            "databricks__create_table_as",
            temporary,
            template_bundle.relation,
            sql,
        )

    def test_macros_create_table_as(self, template_bundle):
        sql = self.render_create_table_as(template_bundle)
        assert sql == f"create or replace table {template_bundle.relation} using delta as select 1"

    def test_macros_create_table_as_file_format(self, config, template_bundle):
        config["file_format"] = "parquet"
        config["location_root"] = "/mnt/root"
        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create table {template_bundle.relation} using parquet location "
            f"'/mnt/root/{template_bundle.relation.identifier}' as select 1"
        )
        assert sql == expected

    def test_macros_create_table_as_options(self, config, template_bundle):
        config["options"] = {"compression": "gzip"}
        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create or replace table {template_bundle.relation} "
            'using delta options (compression "gzip" ) as select 1'
        )

        assert sql == expected

    def test_macros_create_table_as_partition(self, config, template_bundle):
        config["partition_by"] = "partition_1"
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation} using delta"
            " partitioned by (partition_1) as select 1"
        )
        assert sql == expected

    def test_macros_create_table_as_partitions(self, config, template_bundle):
        config["partition_by"] = ["partition_1", "partition_2"]
        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create or replace table {template_bundle.relation} "
            "using delta partitioned by (partition_1,partition_2) as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_cluster(self, config, template_bundle):
        config["clustered_by"] = "cluster_1"
        config["buckets"] = "1"
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation} "
            "using delta clustered by (cluster_1) into 1 buckets as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_clusters(self, config, template_bundle):
        config["clustered_by"] = ["cluster_1", "cluster_2"]
        config["buckets"] = "1"
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation} "
            "using delta clustered by (cluster_1,cluster_2) into 1 buckets as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_liquid_cluster(self, config, template_bundle):
        config["liquid_clustered_by"] = "cluster_1"
        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create or replace table {template_bundle.relation} using"
            " delta cluster by (cluster_1) as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_liquid_clusters(self, config, template_bundle):
        config["liquid_clustered_by"] = ["cluster_1", "cluster_2"]
        config["buckets"] = "1"
        sql = self.render_create_table_as(template_bundle)
        expected = (
            f"create or replace table {template_bundle.relation} "
            "using delta cluster by (cluster_1,cluster_2) as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_comment(self, config, template_bundle):
        config["persist_docs"] = {"relation": True}
        template_bundle.context["model"].description = "Description Test"

        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation} "
            "using delta comment 'Description Test' as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_tblproperties(self, config, template_bundle):
        config["tblproperties"] = {"delta.appendOnly": "true"}
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation} "
            "using delta tblproperties ('delta.appendOnly' = 'true' ) as select 1"
        )

        assert sql == expected

    def test_macros_create_table_as_all_delta(self, config, template_bundle):
        config["location_root"] = "/mnt/root"
        config["partition_by"] = ["partition_1", "partition_2"]
        config["liquid_clustered_by"] = ["cluster_1", "cluster_2"]
        config["clustered_by"] = ["cluster_1", "cluster_2"]
        config["buckets"] = "1"
        config["persist_docs"] = {"relation": True}
        config["tblproperties"] = {"delta.appendOnly": "true"}
        template_bundle.context["model"].description = "Description Test"

        config["file_format"] = "delta"
        sql = self.render_create_table_as(template_bundle)

        expected = (
            f"create or replace table {template_bundle.relation} "
            "using delta "
            "partitioned by (partition_1,partition_2) "
            "cluster by (cluster_1,cluster_2) "
            "clustered by (cluster_1,cluster_2) into 1 buckets "
            "location '/mnt/root/some_table' "
            "comment 'Description Test' "
            "tblproperties ('delta.appendOnly' = 'true' ) "
            "as select 1"
        )

        assert sql == expected
