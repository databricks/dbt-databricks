from collections import Counter
from dataclasses import dataclass, field
from importlib.metadata import version as _pkg_version
from typing import Any, Callable, Optional

from dbt.adapters.databricks.__version__ import version as _adapter_version
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.spog.extract import extract_workspace_id
from dbt.adapters.databricks.telemetry import models

# Mirrored from impl.py to avoid an import cycle.
_BEHAVIOR_FLAGS = (
    "use_user_folder_for_python",
    "use_materialization_v2",
    "use_replace_on_for_insert_overwrite",
    "use_managed_iceberg",
    "use_concurrent_microbatch",
    "use_describe_as_json_for_relation_metadata",
)

_COMMAND_MAP = {
    "run": models.DbtCommand.RUN,
    "build": models.DbtCommand.BUILD,
    "test": models.DbtCommand.TEST,
    "seed": models.DbtCommand.SEED,
    "snapshot": models.DbtCommand.SNAPSHOT,
    "compile": models.DbtCommand.COMPILE,
    "docs": models.DbtCommand.DOCS,
    "clone": models.DbtCommand.CLONE,
    "retry": models.DbtCommand.RETRY,
    "show": models.DbtCommand.SHOW,
    "list": models.DbtCommand.LIST,
    "source": models.DbtCommand.SOURCE,
    "run_operation": models.DbtCommand.RUN_OPERATION,
}

_MATERIALIZATION_MAP = {
    "table": models.Materialization.TABLE,
    "view": models.Materialization.VIEW,
    "incremental": models.Materialization.INCREMENTAL,
    "ephemeral": models.Materialization.EPHEMERAL,
    "materialized_view": models.Materialization.MATERIALIZED_VIEW,
    "streaming_table": models.Materialization.STREAMING_TABLE,
    "metric_view": models.Materialization.METRIC_VIEW,
}

_LANGUAGE_MAP = {
    "sql": models.Language.SQL,
    "python": models.Language.PYTHON,
}

_INCREMENTAL_STRATEGY_MAP = {
    "merge": models.IncrementalStrategy.MERGE,
    "append": models.IncrementalStrategy.APPEND,
    "delete+insert": models.IncrementalStrategy.DELETE_INSERT,
    "delete_insert": models.IncrementalStrategy.DELETE_INSERT,
    "insert_overwrite": models.IncrementalStrategy.INSERT_OVERWRITE,
    "replace_where": models.IncrementalStrategy.REPLACE_WHERE,
    "microbatch": models.IncrementalStrategy.MICROBATCH,
}

_PYTHON_SUBMISSION_METHOD_MAP = {
    "serverless_cluster": models.PythonSubmissionMethod.SERVERLESS_CLUSTER,
    "job_cluster": models.PythonSubmissionMethod.JOB_CLUSTER,
    "all_purpose_cluster": models.PythonSubmissionMethod.ALL_PURPOSE_CLUSTER,
    "workflow_job": models.PythonSubmissionMethod.WORKFLOW_JOB,
}

_CATALOG_TYPE_MAP = {
    "unity": models.CatalogType.UNITY_CATALOG,
    "unity_catalog": models.CatalogType.UNITY_CATALOG,
    "hive_metastore": models.CatalogType.HIVE_METASTORE,
}

_FILE_FORMAT_MAP = {
    "delta": models.EffectiveStorageFormat.DELTA,
    "parquet": models.EffectiveStorageFormat.PARQUET,
    "hudi": models.EffectiveStorageFormat.HUDI,
}

_CONSTRAINT_CONFIG_MAP = {
    "not_null": models.ModelConfig.NOT_NULL_CONSTRAINT,
    "check": models.ModelConfig.CHECK_CONSTRAINT,
    "primary_key": models.ModelConfig.PRIMARY_KEY_CONSTRAINT,
    "foreign_key": models.ModelConfig.FOREIGN_KEY_CONSTRAINT,
    "custom": models.ModelConfig.CUSTOM_CONSTRAINT,
}

_STORAGE_FORMAT_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
}

_HMS_CATALOG_NAMES = {"hive_metastore"}

_TAG_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
    models.Materialization.VIEW,
    models.Materialization.MATERIALIZED_VIEW,
    models.Materialization.STREAMING_TABLE,
    models.Materialization.METRIC_VIEW,
}
_COLUMN_TAG_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
    models.Materialization.VIEW,
    models.Materialization.MATERIALIZED_VIEW,
    models.Materialization.STREAMING_TABLE,
}
_LIQUID_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
    models.Materialization.MATERIALIZED_VIEW,
    models.Materialization.STREAMING_TABLE,
}
_ZORDER_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
}
_MASK_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
}
_ROW_FILTER_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
    models.Materialization.MATERIALIZED_VIEW,
    models.Materialization.STREAMING_TABLE,
}
_CONSTRAINT_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
}
_NAMED_COMPUTE_MATERIALIZATIONS = {
    models.Materialization.TABLE,
    models.Materialization.INCREMENTAL,
    models.Materialization.VIEW,
    models.Materialization.MATERIALIZED_VIEW,
    models.Materialization.STREAMING_TABLE,
    models.Materialization.METRIC_VIEW,
}


@dataclass
class _ModelConfigAccumulator:
    scope: models.ModelConfigScope
    model_count: int = 0
    materializations: Counter = field(default_factory=Counter)
    languages: Counter = field(default_factory=Counter)
    incremental_model_count: int = 0
    incremental_strategies: Counter = field(default_factory=Counter)
    storage_formats: Counter = field(default_factory=Counter)
    catalog_types: Counter = field(default_factory=Counter)
    compute_types: Counter = field(default_factory=Counter)
    python_model_count: int = 0
    python_submission_methods: Counter = field(default_factory=Counter)
    config_usage: Counter = field(default_factory=Counter)

    def to_model(self) -> models.ModelConfigStats:
        return models.ModelConfigStats(
            scope=self.scope,
            model_count=self.model_count,
            materialization_counts=_count_rows(
                self.materializations, models.Materialization, models.MaterializationCount
            ),
            language_counts=_count_rows(self.languages, models.Language, models.LanguageCount),
            incremental_model_stats=models.IncrementalModelStats(
                model_count=self.incremental_model_count,
                strategy_counts=_count_rows(
                    self.incremental_strategies,
                    models.IncrementalStrategy,
                    models.IncrementalStrategyCount,
                ),
            ),
            effective_storage_format_counts=_count_rows(
                self.storage_formats,
                models.EffectiveStorageFormat,
                models.EffectiveStorageFormatCount,
            ),
            catalog_type_counts=_count_rows(
                self.catalog_types, models.CatalogType, models.CatalogTypeCount
            ),
            effective_compute_type_counts=_count_rows(
                self.compute_types, models.ComputeType, models.ComputeTypeCount
            ),
            python_model_stats=models.PythonModelStats(
                model_count=self.python_model_count,
                submission_method_counts=_count_rows(
                    self.python_submission_methods,
                    models.PythonSubmissionMethod,
                    models.PythonSubmissionMethodCount,
                ),
            ),
            config_usage=_count_rows(
                self.config_usage, models.ModelConfig, models.ModelConfigUsage
            ),
        )


def classify_compute_type(http_path: Optional[str]) -> models.ComputeType:
    if not http_path:
        return models.ComputeType.TYPE_UNSPECIFIED
    path = http_path.split("?", 1)[0]
    if path and not path.startswith("/"):
        path = f"/{path}"
    if path.startswith(("/sql/1.0/warehouses/", "/sql/1.0/endpoints/")):
        return models.ComputeType.SQL_WAREHOUSE
    if path.startswith("/sql/protocolv1/"):
        return models.ComputeType.ALL_PURPOSE_CLUSTER
    return models.ComputeType.OTHER


def classify_auth_family(creds: DatabricksCredentials) -> models.AuthFamily:
    if getattr(creds, "token", None):
        return models.AuthFamily.PAT
    if getattr(creds, "azure_client_id", None) and getattr(creds, "azure_client_secret", None):
        return models.AuthFamily.AZURE_SERVICE_PRINCIPAL
    if not getattr(creds, "client_secret", None):
        return models.AuthFamily.OAUTH_U2M
    # client_secret is ambiguous between M2M and legacy Azure.
    return models.AuthFamily.LEGACY_CLIENT_SECRET_AMBIGUOUS


def classify_command(which: Optional[str]) -> models.DbtCommand:
    if not which:
        return models.DbtCommand.TYPE_UNSPECIFIED
    token = str(which).strip().lower().replace("-", "_").split()[0]
    return _COMMAND_MAP.get(token, models.DbtCommand.OTHER)


def classify_warn_error_policy(warn_error: Any, warn_error_options: Any) -> models.WarnErrorPolicy:
    # The legacy boolean takes precedence.
    if warn_error:
        return models.WarnErrorPolicy.WARN_ERROR_ALL
    if warn_error_options:
        opts = warn_error_options
        get = lambda name: (  # noqa: E731 - keeps object/dict compatibility together
            opts.get(name) if isinstance(opts, dict) else getattr(opts, name, None)
        )
        error = get("error") or get("include") or []
        warn = get("warn") or get("exclude") or []
        silence = get("silence") or []
        # Named overrides make `error: all` custom.
        if error in ("all", "*") and not warn and not silence:
            return models.WarnErrorPolicy.WARN_ERROR_ALL
        has_policy = bool(error or warn or silence)
        if has_policy:
            return models.WarnErrorPolicy.WARN_ERROR_CUSTOM_POLICY
    return models.WarnErrorPolicy.WARN_ERROR_DISABLED


def _resource_type(node: Any) -> str:
    rt = getattr(node, "resource_type", None)
    return str(getattr(rt, "value", rt))


def _bump(counts: models.ResourceCounts, node: Any, resource_type: str) -> None:
    if resource_type == "model":
        counts.model_count += 1
    elif resource_type == "test":
        counts.data_test_count += 1
        if getattr(node, "test_metadata", None) is not None:
            counts.generic_data_test_count += 1
    elif resource_type == "seed":
        counts.seed_count += 1
    elif resource_type == "snapshot":
        counts.snapshot_count += 1
    elif resource_type == "source":
        counts.source_count += 1
    elif resource_type == "function":
        counts.function_count += 1
    elif resource_type == "exposure":
        counts.exposure_count += 1
    elif resource_type == "saved_query":
        counts.saved_query_count += 1
    elif resource_type == "unit_test":
        counts.unit_test_count += 1
    else:
        counts.other_count += 1


def aggregate_manifest(manifest: Any) -> models.ManifestStats:
    stats = models.ManifestStats()
    project_name = None
    metadata = getattr(manifest, "metadata", None)
    if metadata is not None:
        project_name = getattr(metadata, "project_name", None)

    collections = [
        "nodes",
        "sources",
        "exposures",
        "metrics",
        "semantic_models",
        "saved_queries",
        "functions",
        "unit_tests",
    ]
    for collection in collections:
        items = getattr(manifest, collection, None)
        if not items:
            continue
        for node in items.values():
            resource_type = _resource_type(node)
            _bump(stats.enabled_total, node, resource_type)
            is_root = getattr(node, "package_name", None) == project_name
            _bump(
                stats.enabled_root_project if is_root else stats.enabled_installed_packages,
                node,
                resource_type,
            )
    return stats


def _count_rows(counts: Counter, enum_type: Any, row_type: type) -> list:
    return [row_type(value, counts[value]) for value in enum_type if counts[value]]


def _value(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    getter = getattr(obj, "get", None)
    if getter is not None:
        try:
            return getter(name, default)
        except TypeError:
            value = getter(name)
            return default if value is None else value
    return getattr(obj, name, default)


def _normalized(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower().replace("-", "_").replace(" ", "_")


def _enabled(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _materialization(config: Any) -> models.Materialization:
    value = _normalized(_value(config, "materialized"))
    return _MATERIALIZATION_MAP.get(value, models.Materialization.OTHER)


def _language(node: Any) -> models.Language:
    value = _normalized(getattr(node, "language", "sql"))
    return _LANGUAGE_MAP.get(value, models.Language.OTHER)


def _incremental_strategy(config: Any) -> models.IncrementalStrategy:
    value = _normalized(_value(config, "incremental_strategy") or "merge")
    return _INCREMENTAL_STRATEGY_MAP.get(value, models.IncrementalStrategy.OTHER)


def _python_submission_method(config: Any) -> models.PythonSubmissionMethod:
    value = _normalized(_value(config, "submission_method") or "all_purpose_cluster")
    return _PYTHON_SUBMISSION_METHOD_MAP.get(value, models.PythonSubmissionMethod.OTHER)


def _catalog_type(catalog_relation: Any, node: Any) -> models.CatalogType:
    # Physical hive_metastore is HMS even when the default Unity integration
    # supplies catalog_type="unity".
    physical = _normalized(
        getattr(catalog_relation, "catalog_name", None) if catalog_relation is not None else None
    )
    if not physical:
        physical = _normalized(getattr(node, "database", None))
    if physical in _HMS_CATALOG_NAMES:
        return models.CatalogType.HIVE_METASTORE
    if catalog_relation is None:
        return models.CatalogType.TYPE_UNSPECIFIED
    value = _normalized(getattr(catalog_relation, "catalog_type", None))
    return _CATALOG_TYPE_MAP.get(value, models.CatalogType.OTHER)


def _storage_format(
    catalog_relation: Any, use_managed_iceberg: bool
) -> models.EffectiveStorageFormat:
    if catalog_relation is None:
        return models.EffectiveStorageFormat.TYPE_UNSPECIFIED
    if _normalized(getattr(catalog_relation, "table_format", None)) == "iceberg":
        if use_managed_iceberg:
            return models.EffectiveStorageFormat.MANAGED_ICEBERG
        return models.EffectiveStorageFormat.UNIFORM_ICEBERG
    file_format = _normalized(getattr(catalog_relation, "file_format", None))
    if not file_format:
        return models.EffectiveStorageFormat.TYPE_UNSPECIFIED
    return _FILE_FORMAT_MAP.get(file_format, models.EffectiveStorageFormat.OTHER)


def _compute_type(config: Any, creds: DatabricksCredentials) -> models.ComputeType:
    compute_name = _value(config, "databricks_compute")
    if not compute_name:
        return classify_compute_type(getattr(creds, "http_path", None))
    compute = getattr(creds, "compute", None) or {}
    compute_config = compute.get(compute_name)
    return classify_compute_type(_value(compute_config, "http_path"))


def _columns(node: Any) -> list[Any]:
    columns = getattr(node, "columns", None) or {}
    return list(columns.values()) if isinstance(columns, dict) else list(columns)


def _column_extra(column: Any) -> dict:
    extra = _value(column, "_extra", {})
    return extra if isinstance(extra, dict) else {}


def _constraint_name(constraint: Any) -> str:
    return _normalized(_value(constraint, "type"))


def _constraint_configs(node: Any, config: Any) -> set[models.ModelConfig]:
    constraint_names = {
        _constraint_name(constraint) for constraint in (getattr(node, "constraints", None) or [])
    }
    columns = _columns(node)
    for column in columns:
        constraint_names.update(
            _constraint_name(constraint) for constraint in (_value(column, "constraints", []) or [])
        )

    if _enabled(_value(config, "persist_constraints")):
        meta = getattr(node, "meta", None) or {}
        for constraint in _value(meta, "constraints", []) or []:
            constraint_type = _constraint_name(constraint)
            constraint_names.add(constraint_type or "check")
        for column in columns:
            legacy_constraint = _value(_value(column, "meta", {}), "constraint")
            if legacy_constraint:
                constraint_names.add(
                    _constraint_name(legacy_constraint) or _normalized(legacy_constraint)
                )

    return {
        model_config
        for name in constraint_names
        if (model_config := _CONSTRAINT_CONFIG_MAP.get(name)) is not None
    }


def _shared_config_usage(
    node: Any, config: Any, materialization: models.Materialization
) -> set[models.ModelConfig]:
    usage: set[models.ModelConfig] = set()
    if materialization in _CONSTRAINT_MATERIALIZATIONS:
        usage.update(_constraint_configs(node, config))
    auto_liquid_cluster = _enabled(_value(config, "auto_liquid_cluster"))
    has_liquid = bool(_value(config, "liquid_clustered_by") or auto_liquid_cluster)
    if has_liquid and materialization in _LIQUID_MATERIALIZATIONS:
        usage.add(models.ModelConfig.LIQUID_CLUSTERING)
        if auto_liquid_cluster:
            usage.add(models.ModelConfig.AUTO_LIQUID_CLUSTERING)
    if _value(config, "zorder") and materialization in _ZORDER_MATERIALIZATIONS and not has_liquid:
        usage.add(models.ModelConfig.ZORDER)
    if _value(config, "databricks_tags") and materialization in _TAG_MATERIALIZATIONS:
        usage.add(models.ModelConfig.DATABRICKS_RELATION_TAGS)
    columns = _columns(node)
    if (
        any(_column_extra(column).get("databricks_tags") for column in columns)
        and materialization in _COLUMN_TAG_MATERIALIZATIONS
    ):
        usage.add(models.ModelConfig.COLUMN_TAGS)
    if (
        any(_column_extra(column).get("column_mask") for column in columns)
        and materialization in _MASK_MATERIALIZATIONS
    ):
        usage.add(models.ModelConfig.COLUMN_MASKS)
    if _value(config, "row_filter") and materialization in _ROW_FILTER_MATERIALIZATIONS:
        usage.add(models.ModelConfig.ROW_FILTER)
    if _value(config, "databricks_compute") and materialization in _NAMED_COMPUTE_MATERIALIZATIONS:
        usage.add(models.ModelConfig.NAMED_COMPUTE_ROUTING)
    return usage


def _incremental_config_usage(config: Any) -> set[models.ModelConfig]:
    usage = set()
    if _enabled(_value(config, "merge_with_schema_evolution")):
        usage.add(models.ModelConfig.MERGE_SCHEMA_EVOLUTION)
    if _value(config, "not_matched_by_source_action"):
        usage.add(models.ModelConfig.MERGE_NOT_MATCHED_BY_SOURCE)
    return usage


def _catalog_relation(node: Any, builder: Callable[[Any], Any]) -> Any:
    try:
        return builder(node)
    except Exception:
        return None


def aggregate_model_configs(
    manifest: Any,
    creds: DatabricksCredentials,
    behavior_flag: Callable[[str], bool],
    catalog_relation_builder: Callable[[Any], Any],
) -> list[models.ModelConfigStats]:
    root_project = getattr(getattr(manifest, "metadata", None), "project_name", None)
    accumulators = {
        models.ModelConfigScope.ROOT_PROJECT: _ModelConfigAccumulator(
            models.ModelConfigScope.ROOT_PROJECT
        ),
        models.ModelConfigScope.INSTALLED_PACKAGES: _ModelConfigAccumulator(
            models.ModelConfigScope.INSTALLED_PACKAGES
        ),
    }
    use_managed_iceberg = bool(behavior_flag("use_managed_iceberg"))
    for node in (getattr(manifest, "nodes", None) or {}).values():
        if _resource_type(node) != "model":
            continue
        scope = (
            models.ModelConfigScope.ROOT_PROJECT
            if getattr(node, "package_name", None) == root_project
            else models.ModelConfigScope.INSTALLED_PACKAGES
        )
        acc = accumulators[scope]
        config = getattr(node, "config", None)
        materialization = _materialization(config)
        language = _language(node)

        acc.model_count += 1
        acc.materializations[materialization] += 1
        acc.languages[language] += 1
        acc.config_usage.update(_shared_config_usage(node, config, materialization))

        if materialization == models.Materialization.INCREMENTAL:
            acc.incremental_model_count += 1
            acc.incremental_strategies[_incremental_strategy(config)] += 1
            acc.config_usage.update(_incremental_config_usage(config))

        if language == models.Language.PYTHON:
            acc.python_model_count += 1
            acc.python_submission_methods[_python_submission_method(config)] += 1

        if materialization != models.Materialization.EPHEMERAL:
            relation = _catalog_relation(node, catalog_relation_builder)
            acc.catalog_types[_catalog_type(relation, node)] += 1
            acc.compute_types[_compute_type(config, creds)] += 1
            if materialization in _STORAGE_FORMAT_MATERIALIZATIONS:
                acc.storage_formats[_storage_format(relation, use_managed_iceberg)] += 1

    return [
        accumulators[models.ModelConfigScope.ROOT_PROJECT].to_model(),
        accumulators[models.ModelConfigScope.INSTALLED_PACKAGES].to_model(),
    ]


def ephemeral_resource_ids(manifest: Any) -> set[str]:
    """Return ephemeral IDs for local counting only."""
    result = set()
    for node in (getattr(manifest, "nodes", None) or {}).values():
        config = getattr(node, "config", None)
        is_ephemeral = bool(getattr(node, "is_ephemeral_model", False)) or (
            getattr(config, "materialized", None) == "ephemeral"
        )
        unique_id = getattr(node, "unique_id", None)
        if is_ephemeral and unique_id:
            result.add(str(unique_id))
    return result


def _get_flags() -> Any:
    try:
        from dbt.flags import get_flags

        return get_flags()
    except Exception:
        return None


def build_invocation_config(config: Any) -> models.InvocationConfig:
    flags = _get_flags()
    thread_count = getattr(config, "threads", None) or getattr(flags, "THREADS", None) or 0
    return models.InvocationConfig(
        thread_count=int(thread_count),
        dbt_command=classify_command(getattr(flags, "WHICH", None)),
        full_refresh=bool(getattr(flags, "FULL_REFRESH", False)),
        empty=bool(getattr(flags, "EMPTY", False)),
        fail_fast=bool(getattr(flags, "FAIL_FAST", False)),
        warn_error_policy=classify_warn_error_policy(
            getattr(flags, "WARN_ERROR", None), getattr(flags, "WARN_ERROR_OPTIONS", None)
        ),
    )


def _profile_http_paths(creds: DatabricksCredentials) -> list[str]:
    paths: list[str] = []
    default = getattr(creds, "http_path", None)
    if default:
        paths.append(default)
    for cfg in (getattr(creds, "compute", None) or {}).values():
        path = cfg.get("http_path") if cfg else None
        if path:
            paths.append(path)
    return paths


def build_connection_config(creds: DatabricksCredentials) -> models.ConnectionConfig:
    http_path = getattr(creds, "http_path", None)
    connection_parameters = getattr(creds, "connection_parameters", None) or {}
    return models.ConnectionConfig(
        default_compute_type=classify_compute_type(http_path),
        configured_auth_family=classify_auth_family(creds),
        named_compute_count=len(getattr(creds, "compute", None) or {}),
        # Parse only the `o` parameter; discard its value.
        spog_routing_configured=any(
            extract_workspace_id(path) is not None for path in _profile_http_paths(creds)
        ),
        use_kernel=bool(connection_parameters.get("use_kernel")),
    )


def build_project_config(behavior_flag: Callable[[str], bool]) -> models.ProjectConfig:
    values = {name: bool(behavior_flag(name)) for name in _BEHAVIOR_FLAGS}
    return models.ProjectConfig(**values)


def build_post_parse_log(
    manifest: Any,
    config: Any,
    creds: DatabricksCredentials,
    behavior_flag: Callable[[str], bool],
    catalog_relation_builder: Callable[[Any], Any],
) -> models.TelemetryLog:
    invocation_id = _invocation_id(manifest)
    payload = models.PostParsePayload(
        invocation_config=build_invocation_config(config),
        manifest_stats=aggregate_manifest(manifest),
        connection_config=build_connection_config(creds),
        project_config=build_project_config(behavior_flag),
        model_config_stats=aggregate_model_configs(
            manifest,
            creds,
            behavior_flag,
            catalog_relation_builder,
        ),
    )
    return models.TelemetryLog(
        invocation_id=invocation_id,
        adapter_version=_adapter_version,
        dbt_core_version=_dbt_core_version(),
        post_parse=payload,
    )


def _invocation_id(manifest: Any) -> str:
    try:
        from dbt_common.invocation import get_invocation_id

        invocation_id = get_invocation_id()
        if invocation_id:
            return str(invocation_id)
    except Exception:
        pass
    metadata = getattr(manifest, "metadata", None)
    return str(getattr(metadata, "invocation_id", "") or "")


def _dbt_core_version() -> str:
    try:
        return _pkg_version("dbt-core")
    except Exception:
        return ""


_STATUS_ATTR = {
    "success": "success",
    "error": "error",
    "fail": "fail",
    "warn": "warn",
    "skipped": "skipped",
    "partial_success": "partial_success",
    "pass": "pass_",
    "runtime_error": "runtime_error",
    "no_op": "no_op",
    "reused": "reused",
}
_STATUS_BUCKETS = tuple(dict.fromkeys(_STATUS_ATTR.values()))

_RESOURCE_TYPE = {
    "model": models.ResourceType.MODEL,
    "test": models.ResourceType.DATA_TEST,
    "unit_test": models.ResourceType.UNIT_TEST,
    "seed": models.ResourceType.SEED,
    "snapshot": models.ResourceType.SNAPSHOT,
    "source": models.ResourceType.SOURCE,
    "function": models.ResourceType.FUNCTION,
    "exposure": models.ResourceType.EXPOSURE,
    "saved_query": models.ResourceType.SAVED_QUERY,
}

_AUXILIARY_TYPES = {"operation", "hook"}


def _norm(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_").replace(" ", "_")


def _set_total(counts: models.NodeStatusCounts) -> None:
    counts.total = sum(getattr(counts, b) for b in _STATUS_BUCKETS)


def _bump_status(counts: models.NodeStatusCounts, status: Any) -> bool:
    attr = _STATUS_ATTR.get(_norm(status))
    if attr is None:
        return False
    setattr(counts, attr, getattr(counts, attr) + 1)
    return True


def _resource_from_uid(unique_id: Any) -> str:
    return _norm(str(unique_id).split(".", 1)[0])


def aggregate_node_results(results: list) -> tuple:
    result_counts = models.NodeStatusCounts()
    auxiliary = models.NodeStatusCounts()
    by_type: dict = {}
    unknown = 0
    for unique_id, status in results:
        rtype = _resource_from_uid(unique_id)
        if rtype in _AUXILIARY_TYPES:
            _bump_status(auxiliary, status)
            continue
        if not _bump_status(result_counts, status):
            continue
        enum = _RESOURCE_TYPE.get(rtype)
        if enum is None:
            unknown += 1
        else:
            _bump_status(by_type.setdefault(enum, models.NodeStatusCounts()), status)
    _set_total(result_counts)
    _set_total(auxiliary)
    results_by_resource_type = []
    for enum, counts in by_type.items():
        _set_total(counts)
        results_by_resource_type.append(
            models.ResourceOutcomeStats(resource_type=enum, status_counts=counts)
        )
    return result_counts, results_by_resource_type, auxiliary, unknown


def _classify_outcome(
    exc_type: Optional[type],
    has_failures: bool,
    fail_fast_triggered: bool,
    task_success: Optional[bool],
) -> tuple[models.InvocationStatus, models.TerminationReason]:
    if exc_type is not None:
        if issubclass(exc_type, (KeyboardInterrupt, SystemExit)):
            return models.InvocationStatus.INTERRUPTED, models.TerminationReason.INTERRUPTED
        try:
            from dbt_common.exceptions import DbtBaseException, DbtInternalError

            if issubclass(exc_type, DbtBaseException) and not issubclass(
                exc_type, DbtInternalError
            ):
                return models.InvocationStatus.HANDLED_ERROR, models.TerminationReason.TASK_ERROR
        except Exception:
            pass
        return models.InvocationStatus.INTERNAL_ERROR, models.TerminationReason.INTERNAL_ERROR
    if task_success is False or (task_success is None and has_failures):
        reason = (
            models.TerminationReason.FAIL_FAST
            if fail_fast_triggered
            else models.TerminationReason.NORMAL
        )
        return models.InvocationStatus.HANDLED_ERROR, reason
    return models.InvocationStatus.SUCCESS, models.TerminationReason.NORMAL


def build_post_run_log(
    invocation_id: str,
    elapsed_ms: int,
    exc_type: Optional[type],
    results: list,
    expected_result_resources: int,
    coverage_complete: bool,
    results_captured: bool,
    selected_resources: Optional[int] = None,
    fail_fast_triggered: bool = False,
    task_success: Optional[bool] = None,
) -> models.TelemetryLog:
    result_counts, by_type, auxiliary, unknown = aggregate_node_results(results)
    has_failures = bool(
        result_counts.error
        or result_counts.fail
        or result_counts.runtime_error
        or result_counts.partial_success
    )
    status, reason = _classify_outcome(
        exc_type,
        has_failures,
        fail_fast_triggered,
        task_success,
    )
    aggregates_available = bool(results_captured)
    return models.TelemetryLog(
        invocation_id=invocation_id,
        adapter_version=_adapter_version,
        dbt_core_version=_dbt_core_version(),
        event_type=models.EventType.POST_RUN,
        post_run=models.PostRunPayload(
            run_outcome=models.RunOutcome(
                invocation_status=status,
                termination_reason=reason,
                invocation_duration_ms=elapsed_ms,
                result_aggregates_available=aggregates_available,
                expected_result_coverage_complete=(
                    coverage_complete if aggregates_available else None
                ),
            ),
            selected_resources=selected_resources,
            expected_result_resources=expected_result_resources,
            result_counts=result_counts if aggregates_available else None,
            results_by_resource_type=by_type if aggregates_available else None,
            auxiliary_hook_results=auxiliary if aggregates_available else None,
            unknown_resource_type_results=unknown if aggregates_available else None,
        ),
    )
