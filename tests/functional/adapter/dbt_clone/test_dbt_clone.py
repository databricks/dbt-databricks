import pytest

from dbt.tests import util
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClone
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClonePossible


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


class TestClonePossible(BaseClonePossible, CleanupMixin):
    pass


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
        assert (
            "Warning: The state and target directories are the same: 'target'" in output
        )
