from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class EventType(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    POST_PARSE = "POST_PARSE"


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


@dataclass
class ResourceCounts:
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
    unit_test_count: int = 0


@dataclass
class ManifestStats:
    enabled_total: ResourceCounts = field(default_factory=ResourceCounts)
    enabled_root_project: ResourceCounts = field(default_factory=ResourceCounts)
    enabled_installed_packages: ResourceCounts = field(default_factory=ResourceCounts)


@dataclass
class InvocationConfig:
    thread_count: int = 0
    dbt_command: DbtCommand = DbtCommand.TYPE_UNSPECIFIED
    full_refresh: bool = False
    empty: bool = False
    fail_fast: bool = False
    warn_error_policy: WarnErrorPolicy = WarnErrorPolicy.WARN_ERROR_DISABLED


@dataclass
class ConnectionConfig:
    default_compute_type: ComputeType = ComputeType.TYPE_UNSPECIFIED
    configured_auth_family: AuthFamily = AuthFamily.TYPE_UNSPECIFIED
    named_compute_count: int = 0
    spog_routing_configured: bool = False
    use_kernel: bool = False


@dataclass
class ProjectConfig:
    use_user_folder_for_python: bool = False
    use_materialization_v2: bool = False
    use_replace_on_for_insert_overwrite: bool = False
    use_managed_iceberg: bool = False
    use_concurrent_microbatch: bool = False
    use_describe_as_json_for_relation_metadata: bool = False


@dataclass
class PostParsePayload:
    invocation_config: InvocationConfig
    manifest_stats: ManifestStats
    connection_config: ConnectionConfig
    project_config: ProjectConfig


@dataclass
class TelemetryLog:
    invocation_id: str
    adapter_version: str
    dbt_core_version: str
    event_type: EventType = EventType.POST_PARSE
    post_parse: Optional[PostParsePayload] = None
