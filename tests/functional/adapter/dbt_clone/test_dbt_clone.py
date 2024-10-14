import pytest

from dbt.tests import util
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClone
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClonePossible
from dbt.tests.util import run_dbt
from tests.functional.adapter.dbt_clone import fixtures


class CleanupMixin:
    @pytest.fixture(autouse=True)
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


@pytest.mark.skip("Skip until tests fixed upstream in 1.8.0 final")
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
        for row in results:
            if row[0] == "comment":
                assert row[1] == "This is a table model"

        results = project.run_sql(
            f"describe extended {project.database}.{other_schema}.view_model",
            fetch="all",
        )
        for row in results:
            if row[0] == "comment":
                assert row[1] == "This is a view model"
