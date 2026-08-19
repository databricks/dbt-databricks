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


def classify_compute_type(http_path: Optional[str]) -> models.ComputeType:
    if not http_path:
        return models.ComputeType.TYPE_UNSPECIFIED
    path = http_path.split("?", 1)[0]
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
    # client_secret can select M2M or legacy Azure auth.
    return models.AuthFamily.LEGACY_CLIENT_SECRET_AMBIGUOUS


def classify_command(which: Optional[str]) -> models.DbtCommand:
    if not which:
        return models.DbtCommand.TYPE_UNSPECIFIED
    token = str(which).strip().lower().replace("-", "_").split()[0]
    return _COMMAND_MAP.get(token, models.DbtCommand.OTHER)


def classify_warn_error_policy(warn_error: Any, warn_error_options: Any) -> models.WarnErrorPolicy:
    # dbt-core gives the legacy boolean precedence.
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
        # Named overrides make `error: all` a custom policy.
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


def ephemeral_resource_ids(manifest: Any) -> set[str]:
    """Return only selected-count metadata; IDs are never serialized."""
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


def build_connection_config(creds: DatabricksCredentials) -> models.ConnectionConfig:
    http_path = getattr(creds, "http_path", None)
    connection_parameters = getattr(creds, "connection_parameters", None) or {}
    return models.ConnectionConfig(
        default_compute_type=classify_compute_type(http_path),
        configured_auth_family=classify_auth_family(creds),
        named_compute_count=len(getattr(creds, "compute", None) or {}),
        # Only the parsed `o` query parameter is recognized; its value is discarded.
        spog_routing_configured=extract_workspace_id(http_path) is not None,
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
) -> models.TelemetryLog:
    invocation_id = _invocation_id(manifest)
    payload = models.PostParsePayload(
        invocation_config=build_invocation_config(config),
        manifest_stats=aggregate_manifest(manifest),
        connection_config=build_connection_config(creds),
        project_config=build_project_config(behavior_flag),
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
    """Build POST_RUN from authoritative or interrupt-fallback results."""
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
