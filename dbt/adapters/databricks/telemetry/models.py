from dataclasses import dataclass, field
from typing import Optional

# Closed-enum value names; must match the proto enum members.
EVENT_TYPE_POST_PARSE = "POST_PARSE"
EVENT_TYPE_POST_RUN = "POST_RUN"

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

RESOURCE_TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
RESOURCE_TYPE_MODEL = "MODEL"
RESOURCE_TYPE_DATA_TEST = "DATA_TEST"
RESOURCE_TYPE_UNIT_TEST = "UNIT_TEST"
RESOURCE_TYPE_SEED = "SEED"
RESOURCE_TYPE_SNAPSHOT = "SNAPSHOT"
RESOURCE_TYPE_SOURCE = "SOURCE"
RESOURCE_TYPE_FUNCTION = "FUNCTION"
RESOURCE_TYPE_EXPOSURE = "EXPOSURE"
RESOURCE_TYPE_SAVED_QUERY = "SAVED_QUERY"
RESOURCE_TYPE_OTHER = "OTHER"

INVOCATION_STATUS_UNSPECIFIED = "TYPE_UNSPECIFIED"
INVOCATION_STATUS_SUCCESS = "SUCCESS"
INVOCATION_STATUS_HANDLED_ERROR = "HANDLED_ERROR"
INVOCATION_STATUS_INTERRUPTED = "INTERRUPTED"
INVOCATION_STATUS_INTERNAL_ERROR = "INTERNAL_ERROR"

TERMINATION_REASON_UNSPECIFIED = "TYPE_UNSPECIFIED"
TERMINATION_REASON_NORMAL = "NORMAL"
TERMINATION_REASON_FAIL_FAST = "FAIL_FAST"
TERMINATION_REASON_INTERRUPTED = "INTERRUPTED"
TERMINATION_REASON_TASK_ERROR = "TASK_ERROR"
TERMINATION_REASON_INTERNAL_ERROR = "INTERNAL_ERROR"


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
class NodeStatusCounts:
    """Node counts by dbt terminal status; total equals the sum of the buckets."""

    total: int = 0
    success: int = 0
    error: int = 0
    fail: int = 0
    warn: int = 0
    skipped: int = 0
    partial_success: int = 0
    pass_: int = 0  # proto field: pass
    runtime_error: int = 0
    no_op: int = 0
    reused: int = 0


@dataclass
class ResourceOutcomeStats:
    """Status counts for one resource type."""

    resource_type: str = RESOURCE_TYPE_UNSPECIFIED
    status_counts: NodeStatusCounts = field(default_factory=NodeStatusCounts)


@dataclass
class RunOutcome:
    invocation_status: str = INVOCATION_STATUS_UNSPECIFIED
    termination_reason: str = TERMINATION_REASON_UNSPECIFIED
    invocation_duration_ms: int = 0
    result_aggregates_available: bool = False
    expected_result_coverage_complete: bool = False


@dataclass
class PostRunPayload:
    """The ``DbtPostRun`` payload."""

    run_outcome: RunOutcome = field(default_factory=RunOutcome)
    selected_resources: int = 0
    expected_result_resources: int = 0
    result_counts: NodeStatusCounts = field(default_factory=NodeStatusCounts)
    results_by_resource_type: list[ResourceOutcomeStats] = field(default_factory=list)
    auxiliary_hook_results: NodeStatusCounts = field(default_factory=NodeStatusCounts)
    unknown_resource_type_results: int = 0


@dataclass
class TelemetryLog:
    """A ``DbtDatabricksTelemetryLog`` event; one phase payload is populated."""

    invocation_id: str
    adapter_version: str
    dbt_core_version: str
    event_type: str = EVENT_TYPE_POST_PARSE
    post_parse: Optional[PostParsePayload] = None
    post_run: Optional[PostRunPayload] = None
