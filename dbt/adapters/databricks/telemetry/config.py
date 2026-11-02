from typing import Any, Optional

from databricks.sql.client import Connection
from databricks.sql.common.feature_flag import FeatureFlagsContextFactory
from dbt.adapters.databricks.credentials import DatabricksCredentials

ENABLE_FLAG = "enable_dbt_telemetry"
FORCE_ENABLE_FLAG = "force_enable_dbt_telemetry"
SERVER_ENABLE_FLAG = (
    "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForDbtDatabricks"
)
ELIGIBLE_COMMANDS = {"build", "run", "test", "seed", "snapshot"}


def _as_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _connection_parameters(credentials: Optional[DatabricksCredentials]) -> dict[str, Any]:
    if credentials is None:
        return {}
    return credentials.connection_parameters or {}


def is_enabled(credentials: Optional[DatabricksCredentials]) -> bool:
    return _as_bool(_connection_parameters(credentials).get(ENABLE_FLAG))


def is_force_enabled(credentials: Optional[DatabricksCredentials]) -> bool:
    return _as_bool(_connection_parameters(credentials).get(FORCE_ENABLE_FLAG))


def is_collection_enabled(credentials: Optional[DatabricksCredentials]) -> bool:
    return is_force_enabled(credentials) or is_enabled(credentials)


def is_enabled_for_connection(
    credentials: Optional[DatabricksCredentials], connection: Optional[Connection]
) -> bool:
    if credentials is None or connection is None:
        return False
    if is_force_enabled(credentials):
        return True
    if not is_enabled(credentials):
        return False
    try:
        context = FeatureFlagsContextFactory.get_instance(connection)
        value = context.get_flag_value(SERVER_ENABLE_FLAG, default_value=False)
        return _as_bool(value)
    except Exception:
        return False


def is_eligible_command() -> bool:
    try:
        from dbt.flags import get_flags

        which = getattr(get_flags(), "WHICH", None)
        command = str(which or "").strip().lower().replace("_", "-").split()[0]
        return command in ELIGIBLE_COMMANDS
    except Exception:
        return False


def is_collection_enabled_for_invocation(
    credentials: Optional[DatabricksCredentials],
) -> bool:
    return is_collection_enabled(credentials) and is_eligible_command()


def has_reusable_transport(credentials: Optional[DatabricksCredentials]) -> bool:
    """Kernel OAuth U2M credentials are not reusable."""
    if credentials is None:
        return False
    params = credentials.connection_parameters or {}
    kernel_u2m = (
        bool(params.get("use_kernel"))
        and credentials.auth_type == "oauth"
        and not credentials.token
        and not credentials.client_secret
        and not credentials.azure_client_secret
    )
    return not kernel_u2m
