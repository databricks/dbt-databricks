from typing import Optional

from dbt.adapters.databricks.credentials import DatabricksCredentials

ENABLE_FLAG = "enable_dbt_telemetry"
ELIGIBLE_COMMANDS = {"build", "run", "test", "seed", "snapshot"}


def is_enabled(credentials: Optional[DatabricksCredentials]) -> bool:
    if credentials is None:
        return False
    params = credentials.connection_parameters or {}
    return bool(params.get(ENABLE_FLAG, False))


def is_eligible_command() -> bool:
    try:
        from dbt.flags import get_flags

        which = getattr(get_flags(), "WHICH", None)
        command = str(which or "").strip().lower().replace("_", "-").split()[0]
        return command in ELIGIBLE_COMMANDS
    except Exception:
        return False


def is_enabled_for_invocation(credentials: Optional[DatabricksCredentials]) -> bool:
    return is_enabled(credentials) and is_eligible_command()


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
