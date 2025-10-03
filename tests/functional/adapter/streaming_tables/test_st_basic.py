from typing import Optional

import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.tests import util
from dbt.tests.adapter.materialized_view.files import MY_SEED, MY_TABLE, MY_VIEW
from tests.functional.adapter.streaming_tables import fixtures


class TestStreamingTablesMixin:
    @staticmethod
    def insert_record(project, table: BaseRelation, record: tuple[int, int]):
        project.run_sql(f"insert into {table} values {record}")

    @staticmethod
    def refresh_streaming_table():
        util.run_dbt(["run", "--models", "my_streaming_table"])

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        return project.run_sql(f"select count(*) from {relation}", fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return fixtures.query_relation_type(project, relation)

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_streaming_table.sql": fixtures.streaming_table,
            "complex_types_streaming_table.sql": fixtures.complex_types_streaming_table,
            "schema.yml": fixtures.streaming_table_schema,
        }

    @pytest.fixture(scope="class")
    def my_streaming_table(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_streaming_table",
            schema=project.test_schema,
            database=project.database,
            type=DatabricksRelationType.StreamingTable,
        )

    @pytest.fixture(scope="class")
    def my_view(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_view",
            schema=project.test_schema,
            database=project.database,
            type=DatabricksRelationType.View,
        )

    @pytest.fixture(scope="class")
    def my_table(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_table",
            schema=project.test_schema,
            database=project.database,
            type=DatabricksRelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_seed(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_seed",
            schema=project.test_schema,
            database=project.database,
            type=DatabricksRelationType.Table,
        )

    @staticmethod
    def swap_table_to_streaming_table(project, table):
        initial_model = util.get_model_file(project, table)
        new_model = initial_model.replace(
            "materialized='table'", "materialized='streaming_table'"
        ).replace("from", "from stream")
        util.set_model_file(project, table, new_model)

    @staticmethod
    def swap_view_to_streaming_table(project, view):
        initial_model = util.get_model_file(project, view)
        new_model = initial_model.replace(
            "materialized='view'", "materialized='streaming_table'"
        ).replace("from", "from stream")
        util.set_model_file(project, view, new_model)

    @staticmethod
    def swap_streaming_table_to_table(project, streaming_table):
        initial_model = util.get_model_file(project, streaming_table)
        new_model = initial_model.replace(
            "materialized='streaming_table'", "materialized='table'"
        ).replace("from stream", "from")
        util.set_model_file(project, streaming_table, new_model)

    @staticmethod
    def swap_streaming_table_to_view(project, streaming_table):
        initial_model = util.get_model_file(project, streaming_table)
        new_model = initial_model.replace(
            "materialized='streaming_table'", "materialized='view'"
        ).replace("from stream", "from")
        util.set_model_file(project, streaming_table, new_model)

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_streaming_table):
        util.run_dbt(["seed"])
        util.run_dbt(["run", "--models", my_streaming_table.identifier, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = util.get_model_file(project, my_streaming_table)

        yield

        # and then reset them after the test runs
        util.set_model_file(project, my_streaming_table, initial_model)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTablesBasic(TestStreamingTablesMixin):
    def test_streaming_table_create(self, project, my_streaming_table):
        # setup creates it; verify it's there
        assert self.query_relation_type(project, my_streaming_table) == "streaming_table"
        # verify the non-null constraint and column comment are persisted on create
        results = project.run_sql(
            f"""
            SELECT
                is_nullable,
                comment
            FROM {project.database}.information_schema.columns
            WHERE table_catalog = '{project.database}'
              AND table_schema = '{project.test_schema}'
              AND table_name = '{my_streaming_table.identifier}'
              AND column_name = 'id'""",
            fetch="all",
        )
        row = results[0]
        assert row[0] == "NO"
        assert row[1] == "The unique identifier for each record"

    def test_streaming_table_create_idempotent(self, project, my_streaming_table):
        # setup creates it once; verify it's there and run once
        assert self.query_relation_type(project, my_streaming_table) == "streaming_table"
        _, logs = util.run_dbt_and_capture(
            ["--debug", "run", "--models", my_streaming_table.identifier]
        )
        assert self.query_relation_type(project, my_streaming_table) == "streaming_table"
        util.assert_message_in_logs("REFRESHING STREAMING TABLE", logs)

    def test_streaming_table_full_refresh(self, project, my_streaming_table):
        _, logs = util.run_dbt_and_capture(
            [
                "--debug",
                "run",
                "--models",
                my_streaming_table.identifier,
                "--full-refresh",
            ]
        )
        assert self.query_relation_type(project, my_streaming_table) == "streaming_table"
        util.assert_message_in_logs(f"Applying REPLACE to: {my_streaming_table}", logs)

    def test_streaming_table_replaces_table(self, project, my_table):
        util.run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "table"

        self.swap_table_to_streaming_table(project, my_table)

        util.run_dbt(["run", "--models", my_table.identifier])
        # UC doesn't sync metadata fast enough for this to pass consistently
        # assert self.query_relation_type(project, my_table) == "streaming_table"

    def test_streaming_table_replaces_view(self, project, my_view):
        util.run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "view"

        self.swap_view_to_streaming_table(project, my_view)

        util.run_dbt(["run", "--models", my_view.identifier])
        # UC doesn't sync metadata fast enough for this to pass consistently
        # assert self.query_relation_type(project, my_view) == "streaming_table"

    def test_table_replaces_streaming_table(self, project, my_streaming_table):
        util.run_dbt(["run", "--models", my_streaming_table.identifier])
        assert self.query_relation_type(project, my_streaming_table) == "streaming_table"

        self.swap_streaming_table_to_table(project, my_streaming_table)

        util.run_dbt(["run", "--models", my_streaming_table.identifier])
        # UC doesn't sync metadata fast enough for this to pass consistently
        # assert self.query_relation_type(project, my_streaming_table) == "table"

    def test_view_replaces_streaming_table(self, project, my_streaming_table):
        util.run_dbt(["run", "--models", my_streaming_table.identifier])
        assert self.query_relation_type(project, my_streaming_table) == "streaming_table"

        self.swap_streaming_table_to_view(project, my_streaming_table)

        util.run_dbt(["run", "--models", my_streaming_table.identifier])
        # UC doesn't sync metadata fast enough for this to pass consistently
        # assert self.query_relation_type(project, my_streaming_table) == "view"

    def test_streaming_table_only_updates_after_refresh(self, project, my_streaming_table, my_seed):
        # poll database
        table_start = self.query_row_count(project, my_seed)
        view_start = self.query_row_count(project, my_streaming_table)

        # insert new record in table
        self.insert_record(project, my_seed, (4, 400))

        # poll database
        table_mid = self.query_row_count(project, my_seed)
        view_mid = self.query_row_count(project, my_streaming_table)

        # refresh the materialized view
        self.refresh_streaming_table()

        # poll database
        table_end = self.query_row_count(project, my_seed)
        view_end = self.query_row_count(project, my_streaming_table)

        # new records were inserted in the table but didn't show up in the
        # view until it was refreshed
        assert table_start < table_mid == table_end
        assert view_start == view_mid < view_end

    def test_streaming_table_complex_types(self, project):
        full_data_type = (
            "struct<field1:map<string,int>,field2:array<int>,"
            + ",".join([f"field{i}:int" for i in range(3, 31)])
            + ">"
        )
        # Create the complex_types_table needed for the test. Doing this instead of
        # using a seed file because complex types are tricky to represent in a CSV file
        project.run_sql(
            f"""
            CREATE OR REPLACE TABLE complex_types_table (
                my_struct {full_data_type}
            )
            """
        )

        util.run_dbt(["run", "--models", "complex_types_streaming_table"])
        results = project.run_sql(
            f"""
            SELECT COLUMN_NAME, FULL_DATA_TYPE FROM {project.database}.information_schema.columns
            WHERE table_schema = '{project.test_schema}'
            AND table_name = 'complex_types_streaming_table';
            """,
            fetch="all",
        )
        assert results[0][0] == "my_struct"
        assert results[0][1] == full_data_type


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTableLiquidClustering:
    """
    Test liquid clustering support for streaming tables.
    Note: These are smoke tests that verify models can be created with liquid_clustered_by config.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        yield {
            "liquid_clustered_st.sql": fixtures.liquid_clustered_st,
            "schema.yml": fixtures.liquid_clustered_st_schema_v1,
        }

    def test_create_with_liquid_clustering_config(self, project):
        """Test STs can be created with liquid clustering config without errors."""
        # Run seed to create test data (project fixture doesn't run seeds automatically)
        util.run_dbt(["seed"])

        util.run_dbt(["run", "--models", "liquid_clustered_st"])

        # Verify the ST was created successfully by checking the relation type
        relation_type = fixtures.query_relation_type(
            project,
            project.adapter.Relation.create(
                identifier="liquid_clustered_st",
                schema=project.test_schema,
                database=project.database,
                type=DatabricksRelationType.StreamingTable,
            ),
        )
        assert relation_type == "streaming_table"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestStreamingTablesFromFiles(TestStreamingTablesMixin):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_table.sql": fixtures.streaming_table_from_file,
            "schema.yml": fixtures.streaming_table_schema,
        }

    @pytest.mark.skip(reason="Requires external storage configuration that may not be available")
    def test_streaming_table_create_from_file(self, project, my_streaming_table):
        # setup creates it; verify it's there
        assert self.query_relation_type(project, my_streaming_table) == "streaming_table"
        # verify the non-null constraint and column comment are persisted on create
        results = project.run_sql(
            f"""
            SELECT
                is_nullable,
                comment
            FROM {project.database}.information_schema.columns
            WHERE table_catalog = '{project.database}'
                AND table_schema = '{project.test_schema}'
                AND table_name = '{my_streaming_table.identifier}'
                AND column_name = 'id'""",
            fetch="all",
        )
        row = results[0]
        assert row[0] == "NO"
        assert row[1] == "The unique identifier for each record"
