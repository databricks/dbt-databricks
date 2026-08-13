"""Sanitized POST_PARSE payload models, mirroring the DbtDatabricksTelemetryLog
proto. Fixed scalars and closed-enum value names only; no raw credentials,
paths, names, SQL, or open maps.
"""

from dataclasses import dataclass, field

# Closed-enum value names; must match the proto enum members.
EVENT_TYPE_POST_PARSE = "POST_PARSE"

COMPUTE_TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
COMPUTE_TYPE_SQL_WAREHOUSE = "SQL_WAREHOUSE"
COMPUTE_TYPE_ALL_PURPOSE_CLUSTER = "ALL_PURPOSE_CLUSTER"
COMPUTE_TYPE_OTHER = "OTHER"

AUTH_FAMILY_UNSPECIFIED = "TYPE_UNSPECIFIED"
AUTH_FAMILY_PAT = "PAT"
AUTH_FAMILY_OAUTH_U2M = "OAUTH_U2M"
AUTH_FAMILY_OAUTH_M2M = "OAUTH_M2M"
AUTH_FAMILY_AZURE_SERVICE_PRINCIPAL = "AZURE_SERVICE_PRINCIPAL"
AUTH_FAMILY_LEGACY_CLIENT_SECRET_AMBIGUOUS = "LEGACY_CLIENT_SECRET_AMBIGUOUS"
AUTH_FAMILY_OTHER = "OTHER"

COMMAND_UNSPECIFIED = "TYPE_UNSPECIFIED"
COMMAND_OTHER = "OTHER"

WARN_ERROR_DISABLED = "WARN_ERROR_DISABLED"
WARN_ERROR_ALL = "WARN_ERROR_ALL"
WARN_ERROR_CUSTOM_POLICY = "WARN_ERROR_CUSTOM_POLICY"


@dataclass
class ResourceCounts:
    """Per-type enabled-resource counts; reused for whole-manifest totals."""

    model_count: int = 0
    data_test_count: int = 0
    generic_data_test_count: int = 0
    seed_count: int = 0
    snapshot_count: int = 0
    source_count: int = 0
    function_count: int = 0
    exposure_count: int = 0
    saved_query_count: int = 0
    other_count: int = 0


@dataclass
class ManifestStats:
    """Enabled resources in the parsed manifest, split by origin."""

    enabled_total: ResourceCounts = field(default_factory=ResourceCounts)
    enabled_root_project: ResourceCounts = field(default_factory=ResourceCounts)
    enabled_installed_packages: ResourceCounts = field(default_factory=ResourceCounts)


@dataclass
class InvocationConfig:
    """Invocation CLI flags; no customer-defined values."""

    thread_count: int = 0
    dbt_command: str = COMMAND_UNSPECIFIED
    full_refresh: bool = False
    empty: bool = False
    fail_fast: bool = False
    warn_error_policy: str = WARN_ERROR_DISABLED


@dataclass
class ConnectionConfig:
    """Connection classification derived from the profile; no http_path."""

    default_compute_type: str = COMPUTE_TYPE_UNSPECIFIED
    configured_auth_family: str = AUTH_FAMILY_UNSPECIFIED
    named_compute_count: int = 0
    uses_spog_routing: bool = False
    use_kernel: bool = False


@dataclass
class ProjectConfig:
    """dbt-databricks behavior flags in effect for the invocation."""

    use_user_folder_for_python: bool = False
    use_materialization_v2: bool = False
    use_replace_on_for_insert_overwrite: bool = False
    use_managed_iceberg: bool = False
    use_concurrent_microbatch: bool = False
    use_describe_as_json_for_relation_metadata: bool = False


@dataclass
class PostParsePayload:
    """The ``DbtPostParse`` payload."""

    invocation_config: InvocationConfig
    manifest_stats: ManifestStats
    connection_config: ConnectionConfig
    project_config: ProjectConfig


@dataclass
class TelemetryLog:
    """A POST_PARSE ``DbtDatabricksTelemetryLog`` event."""

    invocation_id: str
    adapter_version: str
    dbt_core_version: str
    post_parse: PostParsePayload
    event_type: str = EVENT_TYPE_POST_PARSE
