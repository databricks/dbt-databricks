import os

import pytest

from dbt.tests import util
from dbt.tests.adapter.hooks.test_run_hooks import BaseAfterRunHooks, BasePrePostRunHooks
from tests.functional.adapter.hooks import fixtures as override_fixtures


class TestPrePostRunHooks(BasePrePostRunHooks):
    @pytest.fixture(scope="function")
    def setUp(self, project):
        project.run_sql(f"drop table if exists {project.test_schema}.on_run_hook")
        util.run_sql_with_adapter(project.adapter, override_fixtures.create_table_run_statement)
        project.run_sql(f"drop table if exists {project.test_schema}.schemas")
        project.run_sql(f"drop table if exists {project.test_schema}.db_schemas")
        os.environ["TERM_TEST"] = "TESTING"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.start_hook_order_test ( id int )",
                "drop table {{ target.schema }}.start_hook_order_test",
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.end_hook_order_test ( id int )",
                "drop table {{ target.schema }}.end_hook_order_test",
                "create table {{ target.schema }}.schemas ( schema string )",
                (
                    "insert into {{ target.schema }}.schemas (schema) values "
                    "{% for schema in schemas %}"
                    "( '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}"
                ),
                "create table {{ target.schema }}.db_schemas ( db string, schema string )",
                (
                    "insert into {{ target.schema }}.db_schemas (db, schema) values "
                    "{% for db, schema in database_schemas %}('{{ db }}', '{{ schema }}' )"
                    "{% if not loop.last %},{% endif %}{% endfor %}"
                ),
            ],
            "seeds": {
                "quote_columns": False,
            },
        }

    def get_ctx_vars(self, state, project):
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
            f"select {field_list} from {project.test_schema}.on_run_hook where test_state = "
            f"'{state}'"
        )
        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into on_run_hook table"
        assert len(vals) == 1, "too many rows in hooks table"
        ctx = dict([(k, v) for (k, v) in zip(fields, vals[0])])

        return ctx

    def check_hooks(self, state, project, target):
        ctx = self.get_ctx_vars(state, project)

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
        assert ctx["thread_id"].startswith("Thread-") or ctx["thread_id"] == "MainThread"

    def test_pre_and_post_run_hooks(self, setUp, project, dbt_profile_target):
        util.run_dbt(["run"])

        self.check_hooks("start", project, dbt_profile_target)
        self.check_hooks("end", project, dbt_profile_target)

        util.check_table_does_not_exist(project.adapter, "start_hook_order_test")
        util.check_table_does_not_exist(project.adapter, "end_hook_order_test")
        self.assert_used_schemas(project)

    def test_pre_and_post_seed_hooks(self, setUp, project, dbt_profile_target):
        util.run_dbt(["seed"])

        self.check_hooks("start", project, dbt_profile_target)
        self.check_hooks("end", project, dbt_profile_target)

        util.check_table_does_not_exist(project.adapter, "start_hook_order_test")
        util.check_table_does_not_exist(project.adapter, "end_hook_order_test")
        self.assert_used_schemas(project)


class TestAfterRunHooks(BaseAfterRunHooks):
    pass
