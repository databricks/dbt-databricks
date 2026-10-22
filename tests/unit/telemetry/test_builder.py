from types import SimpleNamespace

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.catalogs._unity import UnityCatalogIntegration
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
    database="main",
    columns=None,
    constraints=None,
    **config,
):
    return SimpleNamespace(
        resource_type="model",
        package_name=package_name,
        language=language,
        database=database,
        config={"materialized": materialized, **config},
        columns=columns or {},
        constraints=constraints or [],
        meta={},
    )


def _unity_delta_relation(_node=None):
    return SimpleNamespace(catalog_type="unity", table_format="default", file_format="delta")


class TestReportedClassifications:
    @pytest.mark.parametrize(
        "creds, expected",
        [
            pytest.param(
                _creds(token="dapi", azure_client_id="a", azure_client_secret="b"),
                models.AuthFamily.PAT,
                id="token_wins_over_azure_sp",
            ),
            pytest.param(
                _creds(azure_client_id="a", azure_client_secret="b"),
                models.AuthFamily.AZURE_SERVICE_PRINCIPAL,
                id="azure_sp",
            ),
            pytest.param(_creds(), models.AuthFamily.OAUTH_U2M, id="no_secret_is_u2m"),
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
            pytest.param(
                True,
                SimpleNamespace(error=[], warn=[], silence=["X"]),
                models.WarnErrorPolicy.WARN_ERROR_ALL,
                id="legacy_takes_precedence",
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
    def test_generic_tests_are_split_from_singular(self):
        manifest = SimpleNamespace(
            metadata=SimpleNamespace(project_name="root"),
            nodes={
                "t_generic": _node("test", test_metadata={"name": "not_null"}),
                "t_singular": _node("test"),
            },
        )
        ms = builder.aggregate_manifest(manifest)
        assert ms.enabled_total.generic_data_test_count == 1
        assert ms.enabled_total.data_test_count == 2


class TestAggregateModelConfigs:
    @pytest.mark.parametrize(
        "use_managed, expected_format",
        [
            pytest.param(False, models.EffectiveStorageFormat.UNIFORM_ICEBERG, id="uniform"),
            pytest.param(True, models.EffectiveStorageFormat.MANAGED_ICEBERG, id="managed"),
        ],
    )
    def test_iceberg_storage_and_unresolved_named_compute(self, use_managed, expected_format):
        manifest = SimpleNamespace(
            metadata=SimpleNamespace(project_name="root"),
            nodes={
                "iceberg": _model("table", table_format="iceberg", databricks_compute="missing")
            },
        )
        relation = SimpleNamespace(table_format="iceberg", file_format="delta")

        root = builder.aggregate_model_configs(
            manifest,
            _creds(compute={}),
            lambda flag: use_managed if flag == "use_managed_iceberg" else False,
            lambda node: relation,
        )[0]

        assert root.effective_storage_format_counts == [
            models.EffectiveStorageFormatCount(expected_format, 1)
        ]
        assert root.effective_compute_type_counts == [
            models.ComputeTypeCount(models.ComputeType.TYPE_UNSPECIFIED, 1)
        ]

    def test_config_usage_unions_all_declared_constraint_sources(self):
        node = _model(
            "view",
            columns={
                "id": {
                    "constraints": [{"type": "foreign_key"}],
                    "meta": {"constraint": "not_null"},
                }
            },
            constraints=[
                {"type": "primary_key"},
                {"type": "check", "expression": "id > 0"},
            ],
        )
        node.meta = {"constraints": [{"name": "legacy", "condition": "id > 0"}]}
        root = builder.aggregate_model_configs(
            SimpleNamespace(
                metadata=SimpleNamespace(project_name="root"),
                nodes={"mixed": node},
            ),
            _creds(),
            lambda flag: False,
            _unity_delta_relation,
        )[0]
        assert {row.config: row.count for row in root.config_usage} == {
            models.ModelConfig.PRIMARY_KEY_CONSTRAINT: 1,
            models.ModelConfig.FOREIGN_KEY_CONSTRAINT: 1,
            models.ModelConfig.NOT_NULL_CONSTRAINT: 1,
            models.ModelConfig.CHECK_CONSTRAINT: 1,
        }

    def test_config_usage_ignores_non_sequence_legacy_constraint_metadata(self):
        invalid = _model("table")
        invalid.meta = {"constraints": True}
        declared = _model("table", constraints=[{"type": "not_null"}])
        root = builder.aggregate_model_configs(
            SimpleNamespace(
                metadata=SimpleNamespace(project_name="root"),
                nodes={"invalid": invalid, "declared": declared},
            ),
            _creds(),
            lambda flag: False,
            _unity_delta_relation,
        )[0]
        assert {row.config: row.count for row in root.config_usage} == {
            models.ModelConfig.NOT_NULL_CONSTRAINT: 1,
        }

    def test_config_usage_counts_conflicting_clustering_declarations(self):
        root = builder.aggregate_model_configs(
            SimpleNamespace(
                metadata=SimpleNamespace(project_name="root"),
                nodes={
                    "both": _model(
                        "table",
                        zorder=["id"],
                        liquid_clustered_by=["id"],
                        auto_liquid_cluster=True,
                    )
                },
            ),
            _creds(),
            lambda flag: False,
            _unity_delta_relation,
        )[0]
        assert {row.config: row.count for row in root.config_usage} == {
            models.ModelConfig.ZORDER: 1,
            models.ModelConfig.LIQUID_CLUSTERING: 1,
            models.ModelConfig.AUTO_LIQUID_CLUSTERING: 1,
        }

    def test_config_usage_counts_merge_options_regardless_of_strategy(self):
        root = builder.aggregate_model_configs(
            SimpleNamespace(
                metadata=SimpleNamespace(project_name="root"),
                nodes={
                    "table": _model(
                        "table",
                        merge_with_schema_evolution=True,
                        not_matched_by_source_action="delete",
                    ),
                    "append": _model(
                        "incremental",
                        incremental_strategy="append",
                        merge_with_schema_evolution=True,
                        not_matched_by_source_action="delete",
                    ),
                    "invalid": _model(
                        "incremental",
                        not_matched_by_source_action="drop",
                    ),
                },
            ),
            _creds(),
            lambda flag: False,
            _unity_delta_relation,
        )[0]
        assert {row.config: row.count for row in root.config_usage} == {
            models.ModelConfig.MERGE_SCHEMA_EVOLUTION: 2,
            models.ModelConfig.MERGE_NOT_MATCHED_BY_SOURCE: 3,
        }

    def test_config_usage_counts_declarations_on_any_materialization(self):
        extra = {
            "id": {
                "_extra": {"column_mask": {"function": "mask_id"}, "databricks_tags": {"k": "v"}}
            }
        }
        row_filter = {"function": "f", "columns": ["id"]}
        root = builder.aggregate_model_configs(
            SimpleNamespace(
                metadata=SimpleNamespace(project_name="root"),
                nodes={
                    "view": _model(
                        "view",
                        zorder=["id"],
                        liquid_clustered_by=["id"],
                        columns=extra,
                        row_filter=row_filter,
                        databricks_tags={"a": "b"},
                        databricks_compute="cluster",
                    ),
                    "ephemeral": _model("ephemeral", databricks_compute="cluster", zorder=["id"]),
                    "py": _model(
                        "table",
                        language="python",
                        auto_liquid_cluster=True,
                        row_filter=row_filter,
                        columns=extra,
                    ),
                },
            ),
            _creds(compute={"cluster": {"http_path": "/sql/protocolv1/o/1/cluster"}}),
            lambda flag: False,
            _unity_delta_relation,
        )[0]
        assert {row.config: row.count for row in root.config_usage} == {
            models.ModelConfig.ZORDER: 2,
            models.ModelConfig.LIQUID_CLUSTERING: 2,
            models.ModelConfig.AUTO_LIQUID_CLUSTERING: 1,
            models.ModelConfig.COLUMN_MASKS: 2,
            models.ModelConfig.ROW_FILTER: 2,
            models.ModelConfig.DATABRICKS_RELATION_TAGS: 1,
            models.ModelConfig.COLUMN_TAGS: 2,
            models.ModelConfig.NAMED_COMPUTE_ROUTING: 2,
        }
        assert {row.compute_type: row.count for row in root.effective_compute_type_counts} == {
            models.ComputeType.ALL_PURPOSE_CLUSTER: 1,
            models.ComputeType.SQL_WAREHOUSE: 1,
        }

    def test_physical_hive_metastore_is_classified_as_hms(self):
        node = _model("table", database="hive_metastore")
        node.schema = "dbt"
        node.identifier = "hms"
        relation = UnityCatalogIntegration(constants.DEFAULT_UNITY_CATALOG).build_relation(node)
        assert relation.catalog_type == "unity"
        assert relation.catalog_name == "hive_metastore"

        root = builder.aggregate_model_configs(
            SimpleNamespace(
                metadata=SimpleNamespace(project_name="root"),
                nodes={"hms": node},
            ),
            _creds(),
            lambda flag: False,
            lambda _: relation,
        )[0]

        assert root.catalog_type_counts == [
            models.CatalogTypeCount(models.CatalogType.HIVE_METASTORE, 1)
        ]

    def test_v2_catalog_database_hive_metastore_is_classified_as_hms(self):
        node = _model("table", database="main")
        node.schema = "analytics"
        node.identifier = "model_one"
        integration = UnityCatalogIntegration(
            SimpleNamespace(
                name="v2_routed_catalog",
                catalog_type="unity",
                catalog_name="logical_catalog_label",
                catalog_database="hive_metastore",
                table_format="default",
                external_volume=None,
                file_format="delta",
                adapter_properties={},
            )
        )
        relation = integration.build_relation(node)
        assert relation.catalog_type == "unity"
        assert relation.catalog_name == "logical_catalog_label"
        assert relation.catalog_database == "hive_metastore"

        root = builder.aggregate_model_configs(
            SimpleNamespace(
                metadata=SimpleNamespace(project_name="root"),
                nodes={"model": node},
            ),
            _creds(),
            lambda flag: False,
            integration.build_relation,
        )[0]

        assert root.catalog_type_counts == [
            models.CatalogTypeCount(models.CatalogType.HIVE_METASTORE, 1)
        ]


class TestBuildPostRunLog:
    @pytest.mark.parametrize(
        "exc_type, results, fail_fast, task_success, status, reason",
        [
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
