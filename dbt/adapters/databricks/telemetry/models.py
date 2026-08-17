from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# Closed enums; values are the proto enum member names.
class EventType(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    POST_PARSE = "POST_PARSE"
    POST_RUN = "POST_RUN"


class ComputeType(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    SQL_WAREHOUSE = "SQL_WAREHOUSE"
    ALL_PURPOSE_CLUSTER = "ALL_PURPOSE_CLUSTER"
    OTHER = "OTHER"


class AuthFamily(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    PAT = "PAT"
    OAUTH_U2M = "OAUTH_U2M"
    OAUTH_M2M = "OAUTH_M2M"
    AZURE_SERVICE_PRINCIPAL = "AZURE_SERVICE_PRINCIPAL"
    LEGACY_CLIENT_SECRET_AMBIGUOUS = "LEGACY_CLIENT_SECRET_AMBIGUOUS"
    OTHER = "OTHER"


class DbtCommand(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    RUN = "RUN"
    BUILD = "BUILD"
    TEST = "TEST"
    SEED = "SEED"
    SNAPSHOT = "SNAPSHOT"
    COMPILE = "COMPILE"
    DOCS = "DOCS"
    CLONE = "CLONE"
    RETRY = "RETRY"
    SHOW = "SHOW"
    LIST = "LIST"
    SOURCE = "SOURCE"
    RUN_OPERATION = "RUN_OPERATION"
    OTHER = "OTHER"


class WarnErrorPolicy(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    WARN_ERROR_DISABLED = "WARN_ERROR_DISABLED"
    WARN_ERROR_ALL = "WARN_ERROR_ALL"
    WARN_ERROR_CUSTOM_POLICY = "WARN_ERROR_CUSTOM_POLICY"


class ResourceType(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    MODEL = "MODEL"
    DATA_TEST = "DATA_TEST"
    UNIT_TEST = "UNIT_TEST"
    SEED = "SEED"
    SNAPSHOT = "SNAPSHOT"
    SOURCE = "SOURCE"
    FUNCTION = "FUNCTION"
    EXPOSURE = "EXPOSURE"
    SAVED_QUERY = "SAVED_QUERY"
    OTHER = "OTHER"


class InvocationStatus(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    SUCCESS = "SUCCESS"
    HANDLED_ERROR = "HANDLED_ERROR"
    INTERRUPTED = "INTERRUPTED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class TerminationReason(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    NORMAL = "NORMAL"
    FAIL_FAST = "FAIL_FAST"
    INTERRUPTED = "INTERRUPTED"
    TASK_ERROR = "TASK_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


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
    dbt_command: DbtCommand = DbtCommand.TYPE_UNSPECIFIED
    full_refresh: bool = False
    empty: bool = False
    fail_fast: bool = False
    warn_error_policy: WarnErrorPolicy = WarnErrorPolicy.WARN_ERROR_DISABLED


@dataclass
class ConnectionConfig:
    """Connection classification derived from the profile; no http_path."""

    default_compute_type: ComputeType = ComputeType.TYPE_UNSPECIFIED
    configured_auth_family: AuthFamily = AuthFamily.TYPE_UNSPECIFIED
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

    resource_type: ResourceType = ResourceType.TYPE_UNSPECIFIED
    status_counts: NodeStatusCounts = field(default_factory=NodeStatusCounts)


@dataclass
class RunOutcome:
    invocation_status: InvocationStatus = InvocationStatus.TYPE_UNSPECIFIED
    termination_reason: TerminationReason = TerminationReason.TYPE_UNSPECIFIED
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
    event_type: EventType = EventType.POST_PARSE
    post_parse: Optional[PostParsePayload] = None
    post_run: Optional[PostRunPayload] = None
