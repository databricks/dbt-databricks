import pytest

from dbt.tests import util
from dbt.tests.adapter.hooks import fixtures
from dbt.tests.adapter.hooks.test_model_hooks import BaseTestPrePost
from tests.functional.adapter.hooks import fixtures as override_fixtures


class TestPrePostModelHooks(BaseTestPrePost):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        util.run_sql_with_adapter(
            project.adapter,
            f"drop table if exists {project.test_schema}.on_model_hook",
        )
        util.run_sql_with_adapter(project.adapter, override_fixtures.create_table_statement)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        override_fixtures.MODEL_PRE_HOOK,
                    ],
                    "post-hook": [
                        override_fixtures.MODEL_POST_HOOK,
                    ],
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": fixtures.models__hooks}

    def get_ctx_vars(self, state, count, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
            "thread_id",
        ]
        field_list = ", ".join([f"{f}" for f in fields])
        query = (
            f"select {field_list} from {project.test_schema}.on_model_hook"
            f" where test_state = '{state}'"
        )
        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into hooks table"
        assert len(vals) >= count, "too few rows in hooks table"
        assert len(vals) <= count, "too many rows in hooks table"
        return [{k: v for k, v in zip(fields, val)} for val in vals]

    def check_hooks(self, state, project, target, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            assert ctx["test_state"] == state
            assert ctx["target_dbname"] == target.get("database", "")
            assert ctx["target_host"] == target.get("host", "")
            assert ctx["target_name"] == "default"
            assert ctx["target_schema"] == project.test_schema
            assert ctx["target_type"] == "databricks"

            assert (
                ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
            ), "run_started_at was not set"
            assert (
                ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
            ), "invocation_id was not set"
            assert ctx["thread_id"].startswith("Thread-")

    def test_pre_and_post_run_hooks(self, project, dbt_profile_target):
        util.run_dbt()
        self.check_hooks("start", project, dbt_profile_target)
        self.check_hooks("end", project, dbt_profile_target)
