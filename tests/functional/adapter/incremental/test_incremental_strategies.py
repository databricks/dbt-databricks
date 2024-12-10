import os

import pytest

from dbt.tests import util
from tests.functional.adapter.incremental import fixtures

# We're only testing parquet with SQL Warehouse to ensure that the tests are not run in parallel
# between warehouse and uc cluster.  This causes issues with external locations.


class IncrementalBase:
    def seed_and_run_twice(self):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])
        util.run_dbt(["run"])


class AppendBase(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "append_expected.csv": fixtures.append_expected,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "append"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "append_model.sql": fixtures.base_model,
        }

    def test_incremental(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(project.adapter, ["append_model", "append_expected"])


class TestAppendDelta(AppendBase):
    pass


@pytest.mark.external
@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_cluster")
class TestAppendParquet(AppendBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "models": {
                "+file_format": "parquet",
                "+location_root": f"{location_root}/parquet_append",
                "+include_full_name_in_path": "true",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestAppendParquetHive(AppendBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+incremental_strategy": "append",
            },
        }


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class InsertOverwriteBase(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "overwrite_expected.csv": fixtures.overwrite_expected,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "insert_overwrite"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "overwrite_model.sql": fixtures.base_model,
        }

    def test_incremental(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(project.adapter, ["overwrite_model", "overwrite_expected"])


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestInsertOverwriteDelta(InsertOverwriteBase):
    pass


@pytest.mark.skip_profile("databricks_uc_sql_endpoint")
class TestInsertOverwriteWithPartitionsDelta(InsertOverwriteBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "insert_overwrite",
                "+partition_by": "id",
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "upsert_expected.csv": fixtures.upsert_expected,
        }

    def test_incremental(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(project.adapter, ["overwrite_model", "upsert_expected"])


@pytest.mark.external
@pytest.mark.skip("This test is not repeatable due to external location")
class TestInsertOverwriteParquet(InsertOverwriteBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "models": {
                "+file_format": "parquet",
                "+location_root": f"{location_root}/parquet_insert_overwrite",
                "+include_full_name_in_path": "true",
                "+incremental_strategy": "insert_overwrite",
            },
        }


@pytest.mark.external
@pytest.mark.skip("This test is not repeatable due to external location")
class TestInsertOverwriteWithPartitionsParquet(InsertOverwriteBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        location_root = os.environ.get("DBT_DATABRICKS_LOCATION_ROOT")
        return {
            "models": {
                "+file_format": "parquet",
                "+location_root": f"{location_root}/parquet_insert_overwrite_partitions",
                "+include_full_name_in_path": "true",
                "+incremental_strategy": "insert_overwrite",
                "+partition_by": "id",
            },
        }


class TestMergeDelta(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "merge_expected.csv": fixtures.upsert_expected,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "merge", "+unique_key": "id"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_model.sql": fixtures.base_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(project.adapter, ["merge_model", "merge_expected"])


class BaseMergeColumnsDelta(IncrementalBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_model.sql": fixtures.upsert_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(project.adapter, ["merge_model", "merge_expected"])


class TestMergeIncludeColumns(BaseMergeColumnsDelta):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "merge_expected.csv": fixtures.partial_upsert_expected,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+merge_update_columns": ["msg"]}}


class TestMergeExcludeColumns(BaseMergeColumnsDelta):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "merge_expected.csv": fixtures.exclude_upsert_expected,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+merge_exclude_columns": ["msg"]}}


class TestReplaceWhere(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "replace_where_expected.csv": fixtures.replace_where_expected,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "replace_where.sql": fixtures.replace_where_model,
        }

    def test_replace_where(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(project.adapter, ["replace_where", "replace_where_expected"])


class TestSkipMatched(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "skip_matched_expected.csv": fixtures.skip_matched_expected,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "skip_matched.sql": fixtures.skip_matched_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(project.adapter, ["skip_matched", "skip_matched_expected"])


class TestSkipNotMatched(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "skip_not_matched_expected.csv": fixtures.skip_not_matched_expected,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "skip_not_matched.sql": fixtures.skip_not_matched_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(
            project.adapter, ["skip_not_matched", "skip_not_matched_expected"]
        )


class TestMatchedAndNotMatchedCondition(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "matching_condition_expected.csv": fixtures.matching_condition_expected,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "matching_condition.sql": fixtures.matching_condition_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(
            project.adapter,
            ["matching_condition", "matching_condition_expected"],
        )


class TestNotMatchedBySourceAndConditionThenDelete(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "not_matched_by_source_expected.csv": fixtures.not_matched_by_source_then_del_expected,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "not_matched_by_source.sql": fixtures.not_matched_by_source_then_delete_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(
            project.adapter,
            ["not_matched_by_source", "not_matched_by_source_expected"],
        )


class TestNotMatchedBySourceAndConditionThenUpdate(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "not_matched_by_source_expected.csv": fixtures.not_matched_by_source_then_upd_expected,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "not_matched_by_source.sql": fixtures.not_matched_by_source_then_update_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(
            project.adapter,
            ["not_matched_by_source", "not_matched_by_source_expected"],
        )


class TestMergeSchemaEvolution(IncrementalBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "merge_schema_evolution_expected.csv": fixtures.merge_schema_evolution_expected,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_schema_evolution.sql": fixtures.merge_schema_evolution_model,
        }

    def test_merge(self, project):
        self.seed_and_run_twice()
        util.check_relations_equal(
            project.adapter,
            ["merge_schema_evolution", "merge_schema_evolution_expected"],
        )
