from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


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


class ModelConfigScope(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    ROOT_PROJECT = "ROOT_PROJECT"
    INSTALLED_PACKAGES = "INSTALLED_PACKAGES"


class Materialization(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    TABLE = "TABLE"
    VIEW = "VIEW"
    INCREMENTAL = "INCREMENTAL"
    EPHEMERAL = "EPHEMERAL"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"
    STREAMING_TABLE = "STREAMING_TABLE"
    METRIC_VIEW = "METRIC_VIEW"
    OTHER = "OTHER"


class Language(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    SQL = "SQL"
    PYTHON = "PYTHON"
    OTHER = "OTHER"


class IncrementalStrategy(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    MERGE = "MERGE"
    APPEND = "APPEND"
    DELETE_INSERT = "DELETE_INSERT"
    INSERT_OVERWRITE = "INSERT_OVERWRITE"
    REPLACE_WHERE = "REPLACE_WHERE"
    MICROBATCH = "MICROBATCH"
    OTHER = "OTHER"


class EffectiveStorageFormat(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    DELTA = "DELTA"
    MANAGED_ICEBERG = "MANAGED_ICEBERG"
    UNIFORM_ICEBERG = "UNIFORM_ICEBERG"
    PARQUET = "PARQUET"
    HUDI = "HUDI"
    OTHER = "OTHER"


class CatalogType(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    UNITY_CATALOG = "UNITY_CATALOG"
    HIVE_METASTORE = "HIVE_METASTORE"
    OTHER = "OTHER"


class PythonSubmissionMethod(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    SERVERLESS_CLUSTER = "SERVERLESS_CLUSTER"
    JOB_CLUSTER = "JOB_CLUSTER"
    ALL_PURPOSE_CLUSTER = "ALL_PURPOSE_CLUSTER"
    WORKFLOW_JOB = "WORKFLOW_JOB"
    OTHER = "OTHER"


class ModelConfig(Enum):
    TYPE_UNSPECIFIED = "TYPE_UNSPECIFIED"
    LIQUID_CLUSTERING = "LIQUID_CLUSTERING"
    AUTO_LIQUID_CLUSTERING = "AUTO_LIQUID_CLUSTERING"
    ZORDER = "ZORDER"
    DATABRICKS_RELATION_TAGS = "DATABRICKS_RELATION_TAGS"
    COLUMN_TAGS = "COLUMN_TAGS"
    COLUMN_MASKS = "COLUMN_MASKS"
    ROW_FILTER = "ROW_FILTER"
    NOT_NULL_CONSTRAINT = "NOT_NULL_CONSTRAINT"
    CHECK_CONSTRAINT = "CHECK_CONSTRAINT"
    PRIMARY_KEY_CONSTRAINT = "PRIMARY_KEY_CONSTRAINT"
    FOREIGN_KEY_CONSTRAINT = "FOREIGN_KEY_CONSTRAINT"
    CUSTOM_CONSTRAINT = "CUSTOM_CONSTRAINT"
    NAMED_COMPUTE_ROUTING = "NAMED_COMPUTE_ROUTING"
    MERGE_SCHEMA_EVOLUTION = "MERGE_SCHEMA_EVOLUTION"
    MERGE_NOT_MATCHED_BY_SOURCE = "MERGE_NOT_MATCHED_BY_SOURCE"


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
class ModelConfigUsage:
    config: ModelConfig = ModelConfig.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class MaterializationCount:
    materialization: Materialization = Materialization.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class LanguageCount:
    language: Language = Language.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class IncrementalStrategyCount:
    incremental_strategy: IncrementalStrategy = IncrementalStrategy.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class EffectiveStorageFormatCount:
    effective_storage_format: EffectiveStorageFormat = EffectiveStorageFormat.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class CatalogTypeCount:
    catalog_type: CatalogType = CatalogType.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class ComputeTypeCount:
    compute_type: ComputeType = ComputeType.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class PythonSubmissionMethodCount:
    submission_method: PythonSubmissionMethod = PythonSubmissionMethod.TYPE_UNSPECIFIED
    count: int = 0


@dataclass
class IncrementalModelStats:
    model_count: int = 0
    strategy_counts: list[IncrementalStrategyCount] = field(default_factory=list)


@dataclass
class PythonModelStats:
    model_count: int = 0
    submission_method_counts: list[PythonSubmissionMethodCount] = field(default_factory=list)


@dataclass
class ModelConfigStats:
    scope: ModelConfigScope = ModelConfigScope.TYPE_UNSPECIFIED
    model_count: int = 0
    materialization_counts: list[MaterializationCount] = field(default_factory=list)
    language_counts: list[LanguageCount] = field(default_factory=list)
    incremental_model_stats: IncrementalModelStats = field(default_factory=IncrementalModelStats)
    effective_storage_format_counts: list[EffectiveStorageFormatCount] = field(default_factory=list)
    catalog_type_counts: list[CatalogTypeCount] = field(default_factory=list)
    effective_compute_type_counts: list[ComputeTypeCount] = field(default_factory=list)
    python_model_stats: PythonModelStats = field(default_factory=PythonModelStats)
    config_usage: list[ModelConfigUsage] = field(default_factory=list)


@dataclass
class PostParsePayload:
    invocation_config: InvocationConfig
    manifest_stats: ManifestStats
    connection_config: ConnectionConfig
    project_config: ProjectConfig
    model_config_stats: list[ModelConfigStats] = field(default_factory=list)


@dataclass
class NodeStatusCounts:
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
    resource_type: ResourceType = ResourceType.TYPE_UNSPECIFIED
    status_counts: NodeStatusCounts = field(default_factory=NodeStatusCounts)


@dataclass
class RunOutcome:
    invocation_status: InvocationStatus = InvocationStatus.TYPE_UNSPECIFIED
    termination_reason: TerminationReason = TerminationReason.TYPE_UNSPECIFIED
    invocation_duration_ms: int = 0
    result_aggregates_available: bool = False
    expected_result_coverage_complete: Optional[bool] = None


@dataclass
class PostRunPayload:
    run_outcome: RunOutcome = field(default_factory=RunOutcome)
    selected_resources: Optional[int] = None
    expected_result_resources: int = 0
    result_counts: Optional[NodeStatusCounts] = None
    results_by_resource_type: Optional[list[ResourceOutcomeStats]] = None
    auxiliary_hook_results: Optional[NodeStatusCounts] = None
    unknown_resource_type_results: Optional[int] = None


@dataclass
class TelemetryLog:
    invocation_id: str
    adapter_version: str
    dbt_core_version: str
    event_type: EventType = EventType.POST_PARSE
    post_parse: Optional[PostParsePayload] = None
    post_run: Optional[PostRunPayload] = None
