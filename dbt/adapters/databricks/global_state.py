import os
from typing import ClassVar, Optional

from dbt.adapters.databricks.__version__ import version as __version__


class GlobalState:
    """Global state is a bad idea, but since we don't control instantiation, better to have it in a
    single place than scattered throughout the codebase.
    """

    __invocation_env: ClassVar[Optional[str]] = None
    __invocation_env_set: ClassVar[bool] = False

    USER_AGENT = f"dbt-databricks/{__version__}"

    @classmethod
    def get_invocation_env(cls) -> Optional[str]:
        if not cls.__invocation_env_set:
            cls.__invocation_env = os.getenv("DBT_DATABRICKS_INVOCATION_ENV")
            cls.__invocation_env_set = True
        return cls.__invocation_env

    __session_headers: ClassVar[Optional[str]] = None
    __session_headers_set: ClassVar[bool] = False

    @classmethod
    def get_http_session_headers(cls) -> Optional[str]:
        if not cls.__session_headers_set:
            cls.__session_headers = os.getenv("DBT_DATABRICKS_HTTP_SESSION_HEADERS")
            cls.__session_headers_set = True
        return cls.__session_headers

    __describe_char_bypass: ClassVar[Optional[bool]] = None

    @classmethod
    def get_char_limit_bypass(cls) -> bool:
        if cls.__describe_char_bypass is None:
            cls.__describe_char_bypass = (
                os.getenv("DBT_DESCRIBE_TABLE_2048_CHAR_BYPASS", "False").upper() == "TRUE"
            )
        return cls.__describe_char_bypass

    __connector_log_level: ClassVar[Optional[str]] = None

    @classmethod
    def get_connector_log_level(cls) -> str:
        if cls.__connector_log_level is None:
            cls.__connector_log_level = os.getenv(
                "DBT_DATABRICKS_CONNECTOR_LOG_LEVEL", "WARN"
            ).upper()
        return cls.__connector_log_level
