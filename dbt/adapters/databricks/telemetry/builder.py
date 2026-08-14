from importlib.metadata import version as _pkg_version
from typing import Any, Callable, Optional

from dbt.adapters.databricks.__version__ import version as _adapter_version
from dbt.adapters.databricks.credentials import DatabricksCredentials
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
    "run": "RUN",
    "build": "BUILD",
    "test": "TEST",
    "seed": "SEED",
    "snapshot": "SNAPSHOT",
    "compile": "COMPILE",
    "docs": "DOCS",
    "clone": "CLONE",
    "retry": "RETRY",
    "show": "SHOW",
    "list": "LIST",
    "source": "SOURCE",
    "run_operation": "RUN_OPERATION",
}


def classify_compute_type(http_path: Optional[str]) -> str:
    """Only the two canonical http_path shapes classify; else OTHER. Stricter
    than the adapter's permissive cluster-path helper, which mislabels unknowns.
    """
    if not http_path:
        return models.COMPUTE_TYPE_UNSPECIFIED
    path = http_path.split("?", 1)[0]
    if path.startswith(("/sql/1.0/warehouses/", "/sql/1.0/endpoints/")):
        return models.COMPUTE_TYPE_SQL_WAREHOUSE
    if path.startswith("/sql/protocolv1/"):
        return models.COMPUTE_TYPE_ALL_PURPOSE_CLUSTER
    return models.COMPUTE_TYPE_OTHER


def classify_auth_family(creds: DatabricksCredentials) -> str:
    """Mirrors the credential manager's dispatch order, from config fields only.
    A bare client_secret is fallback-capable (manager tries M2M and legacy
    Azure), so it maps to ambiguous rather than OAUTH_M2M.
    """
    if getattr(creds, "token", None):
        return models.AUTH_FAMILY_PAT
    if getattr(creds, "azure_client_id", None) and getattr(creds, "azure_client_secret", None):
        return models.AUTH_FAMILY_AZURE_SERVICE_PRINCIPAL
    if not getattr(creds, "client_secret", None):
        # No secret -> interactive browser (U2M).
        return models.AUTH_FAMILY_OAUTH_U2M
    return models.AUTH_FAMILY_LEGACY_CLIENT_SECRET_AMBIGUOUS


def classify_command(which: Optional[str]) -> str:
    if not which:
        return models.COMMAND_UNSPECIFIED
    # Normalize e.g. run-operation, docs generate, source freshness.
    token = str(which).strip().lower().replace("-", "_").split()[0]
    return _COMMAND_MAP.get(token, models.COMMAND_OTHER)


def classify_warn_error_policy(warn_error: Any, warn_error_options: Any) -> str:
    if warn_error_options:
        # Any include/exclude/silence policy is a custom policy.
        opts = warn_error_options
        has_policy = any(
            bool(getattr(opts, attr, None) or (opts.get(attr) if isinstance(opts, dict) else None))
            for attr in ("include", "error", "warn", "silence", "exclude")
        )
        if has_policy:
            return models.WARN_ERROR_CUSTOM_POLICY
    if warn_error:
        return models.WARN_ERROR_ALL
    return models.WARN_ERROR_DISABLED


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
        # No proto field for unit tests.
        return
    else:
        # metrics, semantic models, and any other enabled type.
        counts.other_count += 1


def aggregate_manifest(manifest: Any) -> models.ManifestStats:
    """Count enabled resources, split root-project vs installed-package.
    manifest.disabled is a separate dict and is never counted.
    """
    stats = models.ManifestStats()
    project_name = None
    metadata = getattr(manifest, "metadata", None)
    if metadata is not None:
        project_name = getattr(metadata, "project_name", None)

    # Executable nodes live in .nodes; other types have their own top-level dicts.
    collections = ["nodes", "sources", "exposures", "metrics", "saved_queries", "functions"]
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
        # `?o=<id>` marks a SPOG unified endpoint; value discarded.
        uses_spog_routing="?o=" in http_path if http_path else False,
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
    """Assemble the complete POST_PARSE event from live runtime objects."""
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
