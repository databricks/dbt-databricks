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
            models.COMPUTE_TYPE_SQL_WAREHOUSE
        )

    def test_endpoints_form_is_warehouse(self):
        assert builder.classify_compute_type("/sql/1.0/endpoints/a") == (
            models.COMPUTE_TYPE_SQL_WAREHOUSE
        )

    def test_all_purpose_cluster(self):
        assert builder.classify_compute_type("/sql/protocolv1/o/1/2") == (
            models.COMPUTE_TYPE_ALL_PURPOSE_CLUSTER
        )

    def test_query_string_ignored(self):
        assert builder.classify_compute_type("/sql/1.0/warehouses/a?o=9") == (
            models.COMPUTE_TYPE_SQL_WAREHOUSE
        )

    def test_unknown_form_is_other(self):
        assert builder.classify_compute_type("/unknown") == models.COMPUTE_TYPE_OTHER

    def test_missing_is_unspecified(self):
        assert builder.classify_compute_type(None) == models.COMPUTE_TYPE_UNSPECIFIED


class TestClassifyAuthFamily:
    def test_token_is_pat(self):
        assert builder.classify_auth_family(_creds(token="dapi")) == models.AUTH_FAMILY_PAT

    def test_azure_service_principal(self):
        assert (
            builder.classify_auth_family(_creds(azure_client_id="a", azure_client_secret="b"))
            == models.AUTH_FAMILY_AZURE_SERVICE_PRINCIPAL
        )

    def test_no_secret_is_u2m(self):
        assert builder.classify_auth_family(_creds(auth_type="oauth")) == (
            models.AUTH_FAMILY_OAUTH_U2M
        )

    def test_client_secret_is_ambiguous(self):
        assert builder.classify_auth_family(_creds(client_id="c", client_secret="s")) == (
            models.AUTH_FAMILY_LEGACY_CLIENT_SECRET_AMBIGUOUS
        )


class TestClassifyCommand:
    def test_known(self):
        assert builder.classify_command("run") == "RUN"
        assert builder.classify_command("build") == "BUILD"

    def test_normalized_forms(self):
        assert builder.classify_command("run-operation") == "RUN_OPERATION"
        assert builder.classify_command("source freshness") == "SOURCE"
        assert builder.classify_command("docs generate") == "DOCS"

    def test_unknown_is_other(self):
        assert builder.classify_command("parse") == models.COMMAND_OTHER

    def test_missing_is_unspecified(self):
        assert builder.classify_command(None) == models.COMMAND_UNSPECIFIED


class TestClassifyWarnErrorPolicy:
    def test_disabled(self):
        assert builder.classify_warn_error_policy(None, None) == models.WARN_ERROR_DISABLED

    def test_all(self):
        assert builder.classify_warn_error_policy(True, None) == models.WARN_ERROR_ALL

    def test_custom_policy(self):
        assert builder.classify_warn_error_policy(False, {"include": ["X"]}) == (
            models.WARN_ERROR_CUSTOM_POLICY
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
        assert cc.default_compute_type == models.COMPUTE_TYPE_ALL_PURPOSE_CLUSTER
        assert cc.configured_auth_family == models.AUTH_FAMILY_LEGACY_CLIENT_SECRET_AMBIGUOUS
        assert cc.named_compute_count == 2
        assert cc.uses_spog_routing is True
        assert cc.use_kernel is True

    def test_defaults_when_bare(self):
        cc = builder.build_connection_config(_creds(token="dapi"))
        assert cc.named_compute_count == 0
        assert cc.uses_spog_routing is False
        assert cc.use_kernel is False


class TestBuildProjectConfig:
    def test_reads_behavior_flags(self):
        on = {"use_materialization_v2", "use_managed_iceberg"}
        pc = builder.build_project_config(lambda name: name in on)
        assert pc.use_materialization_v2 is True
        assert pc.use_managed_iceberg is True
        assert pc.use_user_folder_for_python is False


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
                "ut": _node("unit_test", "root"),
            },
            sources={"s1": _node("source", "root")},
            exposures={"e1": _node("exposure", "root")},
            metrics={"me1": _node("metric", "root")},
            saved_queries={"sq1": _node("saved_query", "root")},
            functions={},
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
        # operation + metric counted; unit_test is skipped.
        assert ms.enabled_total.other_count == 2

    def test_root_vs_installed_split(self):
        ms = builder.aggregate_manifest(self._manifest())
        assert ms.enabled_root_project.model_count == 1
        assert ms.enabled_installed_packages.model_count == 1

    def test_empty_manifest(self):
        empty = SimpleNamespace(metadata=SimpleNamespace(project_name="root"))
        ms = builder.aggregate_manifest(empty)
        assert ms.enabled_total.model_count == 0


class TestBuildPostParseLog:
    def test_assembles_event(self):
        manifest = SimpleNamespace(
            metadata=SimpleNamespace(project_name="root", invocation_id="inv-9"),
            nodes={"m1": _node("model", "root")},
        )
        log = builder.build_post_parse_log(
            manifest=manifest,
            config=SimpleNamespace(threads=8),
            creds=_creds(token="dapi", http_path="/sql/1.0/warehouses/w"),
            behavior_flag=lambda name: False,
        )
        # Live get_invocation_id() when available, else manifest metadata.
        assert isinstance(log.invocation_id, str) and log.invocation_id
        assert log.event_type == models.EVENT_TYPE_POST_PARSE
        assert log.post_parse.invocation_config.thread_count == 8
        assert log.post_parse.connection_config.default_compute_type == (
            models.COMPUTE_TYPE_SQL_WAREHOUSE
        )
