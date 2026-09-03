from types import SimpleNamespace

import pytest

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
    @pytest.mark.parametrize(
        ("http_path", "expected"),
        [
            (
                "/sql/1.0/warehouses/a",
                models.ComputeType.SQL_WAREHOUSE,
            ),
            (
                "/sql/1.0/endpoints/a",
                models.ComputeType.SQL_WAREHOUSE,
            ),
            (
                "/sql/protocolv1/o/1/2",
                models.ComputeType.ALL_PURPOSE_CLUSTER,
            ),
            ("/unknown", models.ComputeType.OTHER),
            (None, models.ComputeType.TYPE_UNSPECIFIED),
            ("sql/1.0/warehouses/a", models.ComputeType.SQL_WAREHOUSE),
            ("sql/protocolv1/o/1/2", models.ComputeType.ALL_PURPOSE_CLUSTER),
        ],
    )
    def test_classifies_compute_type(self, http_path, expected):
        assert builder.classify_compute_type(http_path) == expected


class TestClassifyAuthFamily:
    @pytest.mark.parametrize(
        ("credentials", "expected"),
        [
            ({"token": "dapi"}, models.AuthFamily.PAT),
            (
                {"azure_client_id": "a", "azure_client_secret": "b"},
                models.AuthFamily.AZURE_SERVICE_PRINCIPAL,
            ),
            ({"auth_type": "oauth"}, models.AuthFamily.OAUTH_U2M),
            (
                {"client_id": "c", "client_secret": "s"},
                models.AuthFamily.LEGACY_CLIENT_SECRET_AMBIGUOUS,
            ),
        ],
    )
    def test_classifies_auth_family(self, credentials, expected):
        assert builder.classify_auth_family(_creds(**credentials)) == expected


class TestClassifyCommand:
    @pytest.mark.parametrize(
        ("command", "expected"),
        [
            ("run", models.DbtCommand.RUN),
            ("build", models.DbtCommand.BUILD),
            ("run-operation", models.DbtCommand.RUN_OPERATION),
            ("source freshness", models.DbtCommand.SOURCE),
            ("docs generate", models.DbtCommand.DOCS),
            ("parse", models.DbtCommand.OTHER),
            (None, models.DbtCommand.TYPE_UNSPECIFIED),
        ],
    )
    def test_classifies_command(self, command, expected):
        assert builder.classify_command(command) == expected


class TestClassifyWarnErrorPolicy:
    @pytest.mark.parametrize(
        ("warn_error", "options", "expected"),
        [
            (None, None, models.WarnErrorPolicy.WARN_ERROR_DISABLED),
            (True, None, models.WarnErrorPolicy.WARN_ERROR_ALL),
            (
                False,
                SimpleNamespace(error="all", warn=[], silence=[]),
                models.WarnErrorPolicy.WARN_ERROR_ALL,
            ),
            (
                False,
                SimpleNamespace(error="all", warn=["SomeWarning"], silence=[]),
                models.WarnErrorPolicy.WARN_ERROR_CUSTOM_POLICY,
            ),
            (
                True,
                SimpleNamespace(error=[], warn=[], silence=["SomeWarning"]),
                models.WarnErrorPolicy.WARN_ERROR_ALL,
            ),
            (
                False,
                {"include": ["X"]},
                models.WarnErrorPolicy.WARN_ERROR_CUSTOM_POLICY,
            ),
        ],
    )
    def test_classifies_warn_error_policy(self, warn_error, options, expected):
        assert builder.classify_warn_error_policy(warn_error, options) == expected


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
