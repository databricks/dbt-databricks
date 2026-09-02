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
