from types import SimpleNamespace

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


class TestClassifyComputeType:
    def test_sql_warehouse(self):
        assert builder.classify_compute_type("/sql/1.0/warehouses/a") == (
            models.ComputeType.SQL_WAREHOUSE
        )

    def test_endpoints_form_is_warehouse(self):
        assert builder.classify_compute_type("/sql/1.0/endpoints/a") == (
            models.ComputeType.SQL_WAREHOUSE
        )

    def test_all_purpose_cluster(self):
        assert builder.classify_compute_type("/sql/protocolv1/o/1/2") == (
            models.ComputeType.ALL_PURPOSE_CLUSTER
        )

    def test_query_string_ignored(self):
        assert builder.classify_compute_type("/sql/1.0/warehouses/a?o=9") == (
            models.ComputeType.SQL_WAREHOUSE
        )

    def test_unknown_form_is_other(self):
        assert builder.classify_compute_type("/unknown") == models.ComputeType.OTHER

    def test_missing_is_unspecified(self):
        assert builder.classify_compute_type(None) == models.ComputeType.TYPE_UNSPECIFIED

    def test_optional_leading_slash(self):
        assert builder.classify_compute_type("sql/1.0/warehouses/a") == (
            models.ComputeType.SQL_WAREHOUSE
        )
        assert builder.classify_compute_type("sql/protocolv1/o/1/2") == (
            models.ComputeType.ALL_PURPOSE_CLUSTER
        )


class TestClassifyAuthFamily:
    def test_token_is_pat(self):
        assert builder.classify_auth_family(_creds(token="dapi")) == models.AuthFamily.PAT

    def test_azure_service_principal(self):
        assert (
            builder.classify_auth_family(_creds(azure_client_id="a", azure_client_secret="b"))
            == models.AuthFamily.AZURE_SERVICE_PRINCIPAL
        )

    def test_no_secret_is_u2m(self):
        assert builder.classify_auth_family(_creds(auth_type="oauth")) == (
            models.AuthFamily.OAUTH_U2M
        )

    def test_client_secret_is_ambiguous(self):
        assert builder.classify_auth_family(_creds(client_id="c", client_secret="s")) == (
            models.AuthFamily.LEGACY_CLIENT_SECRET_AMBIGUOUS
        )


class TestClassifyCommand:
    def test_known(self):
        assert builder.classify_command("run") == models.DbtCommand.RUN
        assert builder.classify_command("build") == models.DbtCommand.BUILD

    def test_normalized_forms(self):
        assert builder.classify_command("run-operation") == models.DbtCommand.RUN_OPERATION
        assert builder.classify_command("source freshness") == models.DbtCommand.SOURCE
        assert builder.classify_command("docs generate") == models.DbtCommand.DOCS

    def test_unknown_is_other(self):
        assert builder.classify_command("parse") == models.DbtCommand.OTHER

    def test_missing_is_unspecified(self):
        assert builder.classify_command(None) == models.DbtCommand.TYPE_UNSPECIFIED


class TestClassifyWarnErrorPolicy:
    def test_disabled(self):
        assert builder.classify_warn_error_policy(None, None) == (
            models.WarnErrorPolicy.WARN_ERROR_DISABLED
        )

    def test_all(self):
        assert builder.classify_warn_error_policy(True, None) == (
            models.WarnErrorPolicy.WARN_ERROR_ALL
        )

    def test_error_all_without_overrides_is_all(self):
        options = SimpleNamespace(error="all", warn=[], silence=[])
        assert builder.classify_warn_error_policy(False, options) == (
            models.WarnErrorPolicy.WARN_ERROR_ALL
        )

    def test_error_all_with_named_override_is_custom(self):
        options = SimpleNamespace(error="all", warn=["SomeWarning"], silence=[])
        assert builder.classify_warn_error_policy(False, options) == (
            models.WarnErrorPolicy.WARN_ERROR_CUSTOM_POLICY
        )

    def test_legacy_warn_error_takes_precedence(self):
        options = SimpleNamespace(error=[], warn=[], silence=["SomeWarning"])
        assert builder.classify_warn_error_policy(True, options) == (
            models.WarnErrorPolicy.WARN_ERROR_ALL
        )

    def test_custom_policy(self):
        assert builder.classify_warn_error_policy(False, {"include": ["X"]}) == (
            models.WarnErrorPolicy.WARN_ERROR_CUSTOM_POLICY
        )


class TestBuildConnectionConfig:
    def test_derives_all_fields(self):
        cc = builder.build_connection_config(
            _creds(
                client_id="c",
                client_secret="s",
                http_path="/sql/protocolv1/o/1/2?o=42",
                compute={"a": {}, "b": {}},
                connection_parameters={"use_kernel": True},
            )
        )
        assert cc.default_compute_type == models.ComputeType.ALL_PURPOSE_CLUSTER
        assert cc.configured_auth_family == models.AuthFamily.LEGACY_CLIENT_SECRET_AMBIGUOUS
        assert cc.named_compute_count == 2
        assert cc.spog_routing_configured is True
        assert cc.use_kernel is True

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
    def _manifest(self):
        return SimpleNamespace(
            metadata=SimpleNamespace(project_name="root", invocation_id="inv-1"),
            nodes={
                "m1": _node("model", "root"),
                "m2": _node("model", "dep_pkg"),
                "t_generic": _node("test", "root", test_metadata={"name": "not_null"}),
                "t_singular": _node("test", "root"),
                "sd": _node("seed", "root"),
                "sn": _node("snapshot", "root"),
                "op": _node("operation", "root"),
            },
            sources={"s1": _node("source", "root")},
            exposures={"e1": _node("exposure", "root")},
            metrics={"me1": _node("metric", "root")},
            saved_queries={"sq1": _node("saved_query", "root")},
            functions={},
            semantic_models={"sem1": _node("semantic_model", "root")},
            unit_tests={"ut2": _node("unit_test", "root")},
        )

    def test_total_counts(self):
        ms = builder.aggregate_manifest(self._manifest())
        assert ms.enabled_total.model_count == 2
        assert ms.enabled_total.data_test_count == 2
        assert ms.enabled_total.generic_data_test_count == 1
        assert ms.enabled_total.seed_count == 1
        assert ms.enabled_total.snapshot_count == 1
        assert ms.enabled_total.source_count == 1
        assert ms.enabled_total.exposure_count == 1
        assert ms.enabled_total.saved_query_count == 1
        assert ms.enabled_total.other_count == 3
        assert ms.enabled_total.unit_test_count == 1

    def test_root_vs_installed_split(self):
        ms = builder.aggregate_manifest(self._manifest())
        assert ms.enabled_root_project.model_count == 1
        assert ms.enabled_installed_packages.model_count == 1


class TestBuildPostRunLog:
    def test_success_when_no_exception(self):
        log = builder.build_post_run_log("inv", 250, None, [], 0, True, True)
        assert log.event_type == models.EventType.POST_RUN
        outcome = log.post_run.run_outcome
        assert outcome.invocation_status == models.InvocationStatus.SUCCESS
        assert outcome.termination_reason == models.TerminationReason.NORMAL
        assert outcome.invocation_duration_ms == 250
        assert outcome.result_aggregates_available is True

    def test_handled_error_when_failures(self):
        outcome = builder.build_post_run_log(
            "inv", 1, None, [("model.p.m1", "error")], 1, True, True
        ).post_run.run_outcome
        assert outcome.invocation_status == models.InvocationStatus.HANDLED_ERROR
        assert outcome.termination_reason == models.TerminationReason.NORMAL

    def test_fail_fast_termination_when_observed(self):
        outcome = builder.build_post_run_log(
            "inv",
            1,
            None,
            [("model.p.m1", "error"), ("model.p.m2", "skipped")],
            2,
            True,
            True,
            selected_resources=2,
            fail_fast_triggered=True,
            task_success=False,
        ).post_run.run_outcome
        assert outcome.invocation_status == models.InvocationStatus.HANDLED_ERROR
        assert outcome.termination_reason == models.TerminationReason.FAIL_FAST

    def test_keyboard_interrupt(self):
        outcome = builder.build_post_run_log(
            "inv", 1, KeyboardInterrupt, [], 0, False, True
        ).post_run.run_outcome
        assert outcome.invocation_status == models.InvocationStatus.INTERRUPTED
        assert outcome.termination_reason == models.TerminationReason.INTERRUPTED

    def test_authoritative_task_failure_includes_auxiliary_failures(self):
        outcome = builder.build_post_run_log(
            "inv",
            1,
            None,
            [("operation.p.h", "error"), ("model.p.m", "skipped")],
            1,
            True,
            True,
            task_success=False,
        ).post_run.run_outcome
        assert outcome.invocation_status == models.InvocationStatus.HANDLED_ERROR

    def test_other_exception_is_internal_error(self):
        outcome = builder.build_post_run_log(
            "inv", 1, RuntimeError, [], 0, False, True
        ).post_run.run_outcome
        assert outcome.invocation_status == models.InvocationStatus.INTERNAL_ERROR
        assert outcome.termination_reason == models.TerminationReason.INTERNAL_ERROR

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
    def test_result_counts_and_total(self):
        rc, _, _, unknown = builder.aggregate_node_results(
            [("model.p.a", "success"), ("model.p.b", "error"), ("test.p.t", "pass")]
        )
        assert rc.total == 3
        assert rc.success == 1 and rc.error == 1 and rc.pass_ == 1
        assert unknown == 0

    def test_results_by_resource_type(self):
        _, by_type, _, _ = builder.aggregate_node_results(
            [("model.p.a", "success"), ("model.p.b", "success"), ("test.p.t", "pass")]
        )
        by = {r.resource_type: r.status_counts for r in by_type}
        assert by[models.ResourceType.MODEL].success == 2
        assert by[models.ResourceType.MODEL].total == 2
        assert by[models.ResourceType.DATA_TEST].pass_ == 1

    def test_operations_are_auxiliary(self):
        rc, by_type, aux, _ = builder.aggregate_node_results(
            [("operation.p.hook", "success"), ("model.p.a", "success")]
        )
        assert aux.total == 1 and aux.success == 1
        assert rc.total == 1
        assert [r.resource_type for r in by_type] == [models.ResourceType.MODEL]

    def test_unknown_resource_type(self):
        rc, by_type, _, unknown = builder.aggregate_node_results(
            [("analysis.p.a", "success"), ("model.p.m", "success")]
        )
        assert unknown == 1
        assert rc.total == 2
        assert [r.resource_type for r in by_type] == [models.ResourceType.MODEL]

    def test_unavailable_resource_type_is_unknown(self):
        rc, _, _, unknown = builder.aggregate_node_results([(None, "skipped")])
        assert rc.total == 1
        assert unknown == 1

    def test_invariant_known_plus_unknown_equals_total(self):
        rc, by_type, _, unknown = builder.aggregate_node_results(
            [
                ("model.p.m", "success"),
                ("test.p.t", "pass"),
                ("analysis.p.a", "fail"),
                ("operation.p.h", "success"),
            ]
        )
        known_total = sum(r.status_counts.total for r in by_type)
        assert known_total + unknown == rc.total
