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


def _model(
    materialized,
    *,
    package_name="root",
    language="sql",
    columns=None,
    constraints=None,
    **config,
):
    return SimpleNamespace(
        resource_type="model",
        package_name=package_name,
        language=language,
        config={"materialized": materialized, **config},
        columns=columns or {},
        constraints=constraints or [],
        meta={},
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


class TestAggregateModelConfigs:
    def test_aggregates_scopes_defaults_and_config_adoption(self):
        columns = {
            "id": {
                "constraints": [{"type": "not_null"}],
                "_extra": {
                    "databricks_tags": {"sensitivity": "high"},
                    "column_mask": {"function": "mask_id"},
                },
            }
        }
        manifest = SimpleNamespace(
            metadata=SimpleNamespace(project_name="root"),
            nodes={
                "table": _model(
                    "table",
                    liquid_clustered_by=["id"],
                    auto_liquid_cluster=True,
                    zorder=["id"],
                    databricks_tags={"team": "data"},
                    row_filter={"function": "filter_id", "columns": ["id"]},
                    columns=columns,
                    constraints=[
                        {"type": "check"},
                        {"type": "primary_key"},
                        {"type": "foreign_key"},
                        {"type": "custom"},
                    ],
                ),
                "incremental": _model(
                    "incremental",
                    table_format="iceberg",
                    databricks_compute="cluster",
                    merge_with_schema_evolution=True,
                    not_matched_by_source_action="delete",
                ),
                "python": _model(
                    "view",
                    language="python",
                    submission_method="serverless_cluster",
                ),
                "dependency": _model("ephemeral", package_name="package"),
                "test": _node("test"),
            },
        )
        creds = _creds(compute={"cluster": {"http_path": "/sql/protocolv1/o/1/cluster"}})

        def build_relation(node):
            config = node.config
            return SimpleNamespace(
                catalog_type="unity",
                table_format=config.get("table_format", "default"),
                file_format=config.get("file_format", "delta"),
            )

        stats = builder.aggregate_model_configs(
            manifest,
            creds,
            lambda flag: False,
            build_relation,
        )
        root, installed = stats

        assert root.scope == models.ModelConfigScope.ROOT_PROJECT
        assert root.model_count == 3
        assert {row.materialization: row.count for row in root.materialization_counts} == {
            models.Materialization.TABLE: 1,
            models.Materialization.VIEW: 1,
            models.Materialization.INCREMENTAL: 1,
        }
        assert {row.language: row.count for row in root.language_counts} == {
            models.Language.SQL: 2,
            models.Language.PYTHON: 1,
        }
        assert root.incremental_model_stats.model_count == 1
        assert root.incremental_model_stats.strategy_counts == [
            models.IncrementalStrategyCount(models.IncrementalStrategy.MERGE, 1)
        ]
        assert {
            row.effective_storage_format: row.count for row in root.effective_storage_format_counts
        } == {
            models.EffectiveStorageFormat.DELTA: 1,
            models.EffectiveStorageFormat.UNIFORM_ICEBERG: 1,
        }
        assert root.catalog_type_counts == [
            models.CatalogTypeCount(models.CatalogType.UNITY_CATALOG, 3)
        ]
        assert {row.compute_type: row.count for row in root.effective_compute_type_counts} == {
            models.ComputeType.SQL_WAREHOUSE: 2,
            models.ComputeType.ALL_PURPOSE_CLUSTER: 1,
        }
        assert root.python_model_stats == models.PythonModelStats(
            model_count=1,
            submission_method_counts=[
                models.PythonSubmissionMethodCount(
                    models.PythonSubmissionMethod.SERVERLESS_CLUSTER, 1
                )
            ],
        )
        assert {row.config: row.count for row in root.config_usage} == {
            config: 1
            for config in (
                models.ModelConfig.LIQUID_CLUSTERING,
                models.ModelConfig.AUTO_LIQUID_CLUSTERING,
                models.ModelConfig.ZORDER,
                models.ModelConfig.DATABRICKS_RELATION_TAGS,
                models.ModelConfig.COLUMN_TAGS,
                models.ModelConfig.COLUMN_MASKS,
                models.ModelConfig.ROW_FILTER,
                models.ModelConfig.NOT_NULL_CONSTRAINT,
                models.ModelConfig.CHECK_CONSTRAINT,
                models.ModelConfig.PRIMARY_KEY_CONSTRAINT,
                models.ModelConfig.FOREIGN_KEY_CONSTRAINT,
                models.ModelConfig.CUSTOM_CONSTRAINT,
                models.ModelConfig.NAMED_COMPUTE_ROUTING,
                models.ModelConfig.MERGE_SCHEMA_EVOLUTION,
                models.ModelConfig.MERGE_NOT_MATCHED_BY_SOURCE,
            )
        }

        assert installed.scope == models.ModelConfigScope.INSTALLED_PACKAGES
        assert installed.model_count == 1
        assert installed.materialization_counts == [
            models.MaterializationCount(models.Materialization.EPHEMERAL, 1)
        ]
        assert installed.effective_storage_format_counts == []
        assert installed.catalog_type_counts == []
        assert installed.effective_compute_type_counts == []

    def test_managed_iceberg_and_unresolved_named_compute(self):
        manifest = SimpleNamespace(
            metadata=SimpleNamespace(project_name="root"),
            nodes={
                "iceberg": _model("table", table_format="iceberg", databricks_compute="missing")
            },
        )
        relation = SimpleNamespace(
            catalog_type="hive_metastore", table_format="iceberg", file_format="delta"
        )

        root = builder.aggregate_model_configs(
            manifest,
            _creds(compute={}),
            lambda flag: flag == "use_managed_iceberg",
            lambda node: relation,
        )[0]

        assert root.effective_storage_format_counts == [
            models.EffectiveStorageFormatCount(models.EffectiveStorageFormat.MANAGED_ICEBERG, 1)
        ]
        assert root.catalog_type_counts == [
            models.CatalogTypeCount(models.CatalogType.HIVE_METASTORE, 1)
        ]
        assert root.effective_compute_type_counts == [
            models.ComputeTypeCount(models.ComputeType.TYPE_UNSPECIFIED, 1)
        ]

    def test_legacy_constraints_require_persist_constraints(self):
        legacy = _model(
            "table",
            persist_constraints=True,
            columns={"id": {"meta": {"constraint": "not_null"}}},
        )
        legacy.meta = {"constraints": [{"name": "positive", "condition": "id > 0"}]}
        ignored = _model(
            "table",
            persist_constraints=False,
            columns={"id": {"meta": {"constraint": "not_null"}}},
        )
        ignored.meta = {"constraints": [{"name": "positive", "condition": "id > 0"}]}
        manifest = SimpleNamespace(
            metadata=SimpleNamespace(project_name="root"),
            nodes={"legacy": legacy, "ignored": ignored},
        )
        relation = SimpleNamespace(
            catalog_type="unity", table_format="default", file_format="delta"
        )

        root = builder.aggregate_model_configs(
            manifest,
            _creds(),
            lambda flag: False,
            lambda node: relation,
        )[0]

        assert {row.config: row.count for row in root.config_usage} == {
            models.ModelConfig.NOT_NULL_CONSTRAINT: 1,
            models.ModelConfig.CHECK_CONSTRAINT: 1,
        }


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

    def test_partial_aggregates_keep_counts_and_incomplete_coverage(self):
        post_run = builder.build_post_run_log(
            "inv", 1, None, [("model.p.m1", "success")], 2, False, True
        ).post_run
        outcome = post_run.run_outcome
        assert outcome.result_aggregates_available is True
        assert outcome.expected_result_coverage_complete is False
        assert post_run.result_counts.success == 1
        assert post_run.result_counts.total == 1


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
