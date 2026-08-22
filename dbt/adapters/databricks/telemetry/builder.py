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
        # Parse only the `o` parameter; discard its value.
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
