from types import SimpleNamespace

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.telemetry import builder, models


def _creds(**kw):
    base = dict(
        token=None,
        client_id=None,
        client_secret=None,
        azure_client_id=None,
        azure_client_secret=None,
        auth_type=None,
        http_path="/sql/1.0/warehouses/x",
        compute=None,
        connection_parameters=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _node(resource_type, package_name="root", test_metadata=None):
    return SimpleNamespace(
        resource_type=resource_type, package_name=package_name, test_metadata=test_metadata
    )


class TestReportedClassifications:
    @pytest.mark.parametrize(
        "http_path, expected",
        [
            pytest.param(
                "/sql/1.0/warehouses/a?o=9",
                models.ComputeType.SQL_WAREHOUSE,
                id="warehouse",
            ),
            pytest.param(
                "/sql/1.0/endpoints/a",
                models.ComputeType.SQL_WAREHOUSE,
                id="legacy_endpoint",
            ),
            pytest.param(
                "/sql/protocolv1/o/1/2",
                models.ComputeType.ALL_PURPOSE_CLUSTER,
                id="cluster",
            ),
            pytest.param("/unknown", models.ComputeType.OTHER, id="other"),
            pytest.param(None, models.ComputeType.TYPE_UNSPECIFIED, id="missing"),
        ],
    )
    def test_compute_type(self, http_path, expected):
        assert builder.classify_compute_type(http_path) == expected

    @pytest.mark.parametrize(
        "creds, expected",
        [
            pytest.param(_creds(token="dapi"), models.AuthFamily.PAT, id="pat"),
            pytest.param(
                _creds(azure_client_id="a", azure_client_secret="b"),
                models.AuthFamily.AZURE_SERVICE_PRINCIPAL,
                id="azure_sp",
            ),
            pytest.param(
                _creds(auth_type="oauth"),
                models.AuthFamily.OAUTH_U2M,
                id="u2m",
            ),
            pytest.param(
                _creds(client_id="c", client_secret="s"),
                models.AuthFamily.LEGACY_CLIENT_SECRET_AMBIGUOUS,
                id="ambiguous_secret",
            ),
        ],
    )
    def test_auth_family(self, creds, expected):
        assert builder.classify_auth_family(creds) == expected

    @pytest.mark.parametrize(
        "warn_error, options, expected",
        [
            pytest.param(None, None, models.WarnErrorPolicy.WARN_ERROR_DISABLED, id="disabled"),
            pytest.param(
                True,
                SimpleNamespace(error=[], warn=[], silence=["X"]),
                models.WarnErrorPolicy.WARN_ERROR_ALL,
                id="legacy_takes_precedence",
            ),
            pytest.param(
                False,
                SimpleNamespace(error="all", warn=[], silence=[]),
                models.WarnErrorPolicy.WARN_ERROR_ALL,
                id="error_all",
            ),
            pytest.param(
                False,
                SimpleNamespace(error="all", warn=["SomeWarning"], silence=[]),
                models.WarnErrorPolicy.WARN_ERROR_CUSTOM_POLICY,
                id="named_override",
            ),
        ],
    )
    def test_warn_error_policy(self, warn_error, options, expected):
        assert builder.classify_warn_error_policy(warn_error, options) == expected


class TestBuildConnectionConfig:
    def test_spog_parameter_is_parsed_not_substring_matched(self):
        cc = builder.build_connection_config(
            _creds(token="dapi", http_path="/sql/1.0/warehouses/w?foo=x&o=42")
        )
        assert cc.spog_routing_configured is True

        cc = builder.build_connection_config(
            _creds(token="dapi", http_path="/sql/1.0/warehouses/w?foo=?o=42")
        )
        assert cc.spog_routing_configured is False

    def test_named_compute_o_parameter_sets_spog_flag(self):
        cc = builder.build_connection_config(
            _creds(
                token="dapi",
                http_path="/sql/1.0/warehouses/default",
                compute={"named": {"http_path": "/sql/1.0/warehouses/named?o=42"}},
            )
        )
        assert cc.spog_routing_configured is True


class TestAggregateManifest:
    def test_root_installed_and_test_kinds(self):
        manifest = SimpleNamespace(
            metadata=SimpleNamespace(project_name="root", invocation_id="inv-1"),
            nodes={
                "m1": _node("model", "root"),
                "m2": _node("model", "dep_pkg"),
                "t_generic": _node("test", "root", test_metadata={"name": "not_null"}),
                "t_singular": _node("test", "root"),
                "op": _node("operation", "root"),
            },
            sources={},
            exposures={},
            metrics={},
            saved_queries={},
            functions={},
            semantic_models={},
            unit_tests={},
        )
        ms = builder.aggregate_manifest(manifest)
        assert ms.enabled_root_project.model_count == 1
        assert ms.enabled_installed_packages.model_count == 1
        assert ms.enabled_total.generic_data_test_count == 1
        assert ms.enabled_total.data_test_count == 2
        assert ms.enabled_total.other_count == 1


class TestBuildPostRunLog:
    @pytest.mark.parametrize(
        "exc_type, results, fail_fast, task_success, status, reason",
        [
            pytest.param(
                None,
                [],
                False,
                None,
                models.InvocationStatus.SUCCESS,
                models.TerminationReason.NORMAL,
                id="success",
            ),
            pytest.param(
                None,
                [("model.p.m1", "error")],
                False,
                None,
                models.InvocationStatus.HANDLED_ERROR,
                models.TerminationReason.NORMAL,
                id="result_failure",
            ),
            pytest.param(
                None,
                [("model.p.m1", "error"), ("model.p.m2", "skipped")],
                True,
                False,
                models.InvocationStatus.HANDLED_ERROR,
                models.TerminationReason.FAIL_FAST,
                id="fail_fast",
            ),
            pytest.param(
                KeyboardInterrupt,
                [],
                False,
                None,
                models.InvocationStatus.INTERRUPTED,
                models.TerminationReason.INTERRUPTED,
                id="interrupt",
            ),
            pytest.param(
                DbtRuntimeError,
                [],
                False,
                None,
                models.InvocationStatus.HANDLED_ERROR,
                models.TerminationReason.TASK_ERROR,
                id="handled_dbt_error",
            ),
            pytest.param(
                RuntimeError,
                [],
                False,
                None,
                models.InvocationStatus.INTERNAL_ERROR,
                models.TerminationReason.INTERNAL_ERROR,
                id="internal_error",
            ),
        ],
    )
    def test_outcome_classification(
        self, exc_type, results, fail_fast, task_success, status, reason
    ):
        outcome = builder.build_post_run_log(
            "inv",
            1,
            exc_type,
            results,
            len(results),
            True,
            True,
            fail_fast_triggered=fail_fast,
            task_success=task_success,
        ).post_run.run_outcome
        assert outcome.invocation_status == status
        assert outcome.termination_reason == reason

    def test_authoritative_task_failure_includes_auxiliary_failures(self):
        post_run = builder.build_post_run_log(
            "inv",
            1,
            None,
            [("operation.p.h", "error"), ("model.p.m", "skipped")],
            1,
            True,
            True,
            task_success=False,
        ).post_run
        assert post_run.run_outcome.invocation_status == models.InvocationStatus.HANDLED_ERROR
        assert post_run.auxiliary_hook_results.error == 1
        assert post_run.auxiliary_hook_results.total == 1
        assert post_run.result_counts.error == 0
        assert post_run.result_counts.skipped == 1

    def test_aggregates_unavailable_when_not_captured(self):
        post_run = builder.build_post_run_log("inv", 1, None, [], 0, False, False).post_run
        outcome = post_run.run_outcome
        assert outcome.result_aggregates_available is False
        assert outcome.expected_result_coverage_complete is None
        assert post_run.result_counts is None
        assert post_run.results_by_resource_type is None
        assert post_run.auxiliary_hook_results is None
        assert post_run.unknown_resource_type_results is None


class TestAggregateNodeResults:
    def test_mixed_results_preserve_accounting(self):
        rc, by_type, aux, unknown = builder.aggregate_node_results(
            [
                ("model.p.m", "success"),
                ("test.p.t", "pass"),
                ("analysis.p.a", "fail"),
                ("operation.p.h", "success"),
                (None, "skipped"),
            ]
        )
        assert aux.total == 1 and aux.success == 1
        assert unknown == 2
        assert rc.success == 1 and rc.pass_ == 1 and rc.fail == 1 and rc.skipped == 1
        assert rc.total == 4
        by = {r.resource_type: r.status_counts for r in by_type}
        assert by[models.ResourceType.MODEL].success == 1
        assert by[models.ResourceType.DATA_TEST].pass_ == 1
        assert sum(r.status_counts.total for r in by_type) + unknown == rc.total
