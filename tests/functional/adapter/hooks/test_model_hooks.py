import json

import pytest
from dbt.tests import util
from dbt.tests.adapter.hooks import fixtures
from dbt.tests.adapter.hooks.test_model_hooks import BaseTestPrePost

from tests.functional.adapter.fixtures import RerunSafeMixin
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

            assert ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0, (
                "run_started_at was not set"
            )
            assert ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0, (
                "invocation_id was not set"
            )
            assert ctx["thread_id"].startswith("Thread-")

    def test_pre_and_post_run_hooks(self, project, dbt_profile_target):
        util.run_dbt()
        self.check_hooks("start", project, dbt_profile_target)
        self.check_hooks("end", project, dbt_profile_target)


ORDINARY_HOOK_ROWS = [(1, "pre-default", False), (2, "post-default", True)]
ALL_HOOK_ROWS = [
    (1, "pre-outside", False),
    (2, "pre-default", False),
    (3, "post-default", True),
    (4, "post-outside", True),
]
DLT_MARKS = [
    pytest.mark.dlt,
    pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster"),
]


class NonTransactionalHooksBase(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"record_hook.sql": override_fixtures.record_hook_macros}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("hook_model", "hook_seed", "hook_snapshot", "hook_audit", "hook_source")

    @staticmethod
    def set_flags(project, v2, run_outside_hooks):
        util.update_config_file(
            {
                "flags": {
                    "use_materialization_v2": v2,
                    "use_non_transactional_hooks": run_outside_hooks,
                }
            },
            project.project_root,
            "dbt_project.yml",
        )
        project.run_sql(override_fixtures.hook_audit_sql)
        project.run_sql(override_fixtures.hook_source_sql)

    @staticmethod
    def audit_rows(project):
        rows = project.run_sql(
            "select sequence, phase, relation_exists from {database}.{schema}.hook_audit"
            " order by sequence",
            fetch="all",
        )
        return [tuple(row) for row in rows]


class TestNonTransactionalHooks(NonTransactionalHooksBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hook_model.sql": override_fixtures.hook_model_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"hook_seed.csv": override_fixtures.hook_seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"hook_snapshot.sql": override_fixtures.hook_snapshot_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        hooks = override_fixtures.hooks_with_outside_category
        return {
            "models": {"test": {"hook_model": hooks}},
            "seeds": {"test": {"hook_seed": hooks}},
            "snapshots": {"test": {"hook_snapshot": hooks}},
        }

    @pytest.mark.parametrize("run_outside_hooks", [False, True], ids=["skip", "run"])
    @pytest.mark.parametrize(
        "kind,v2",
        [
            pytest.param(kind, v2, id=f"{kind}-v{2 if v2 else 1}")
            for kind in ("table", "incremental", "view", "seed")
            for v2 in (False, True)
        ]
        + [
            pytest.param("snapshot", False, id="snapshot"),
            pytest.param(
                "metric_view",
                False,
                id="metric-view",
                marks=pytest.mark.skip_profile("databricks_cluster"),
            ),
            pytest.param("materialized_view", False, id="materialized-view", marks=DLT_MARKS),
            pytest.param("streaming_table", False, id="streaming-table", marks=DLT_MARKS),
        ],
    )
    def test_outside_hooks(self, project, kind, v2, run_outside_hooks):
        self.set_flags(project, v2, run_outside_hooks)
        if kind in ("seed", "snapshot"):
            command = [kind, "--select", f"hook_{kind}"]
        else:
            command = ["run", "--vars", json.dumps({"hook_materialization": kind})]
        util.run_dbt(command)

        expected = ALL_HOOK_ROWS if run_outside_hooks else ORDINARY_HOOK_ROWS
        assert self.audit_rows(project) == expected

        relation = f"hook_{kind}" if kind in ("seed", "snapshot") else "hook_model"
        if kind == "metric_view":
            query, expected_rows = "select measure(row_count)", [(1,)]
        else:
            query, expected_rows = "select id, value", [(1, "one")]
        rows = project.run_sql(f"{query} from {{database}}.{{schema}}.{relation}", fetch="all")
        assert [tuple(row) for row in rows] == expected_rows


class TestNonTransactionalHookHelpers(NonTransactionalHooksBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hook_model.sql": override_fixtures.hook_helper_model_sql}

    @pytest.mark.parametrize("run_outside_hooks", [False, True], ids=["skip", "run"])
    def test_before_begin_and_after_commit(self, project, run_outside_hooks):
        self.set_flags(project, False, run_outside_hooks)
        util.run_dbt(["run"])

        expected = ALL_HOOK_ROWS if run_outside_hooks else ORDINARY_HOOK_ROWS
        assert self.audit_rows(project) == expected
