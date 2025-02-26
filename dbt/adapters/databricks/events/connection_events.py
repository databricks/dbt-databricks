from abc import ABC
from typing import Optional

from databricks.sql.client import Connection

from dbt.adapters.databricks.events.base import SQLErrorEvent


class ConnectionEvent(ABC):
    def __init__(self, connection: Optional[Connection], message: str):
        self.message = message
        self.session_id = "Unknown"
        if connection:
            self.session_id = connection.get_session_id_hex() or "Unknown"

    def __str__(self) -> str:
        return f"Connection(session-id={self.session_id}) - {self.message}"


class ConnectionCreateError(ConnectionEvent):
    def __init__(self, exception: Exception):
        super().__init__(
            None, str(SQLErrorEvent(exception, "Exception while trying to create connection"))
        )


class ConnectionWrapperEvent(ABC):
    def __init__(self, description: str, message: str):
        self.message = message
        self.description = description

    def __str__(self) -> str:
        return f"{self.description} - {self.message}"


class ConnectionReset(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Reset connection handle")


class ConnectionReuse(ConnectionWrapperEvent):
    def __init__(self, description: str, prior_name: str):
        super().__init__(description, f"Reusing connection previously named {prior_name}")


class ConnectionCreate(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Creating connection")


class ConnectionIdleClose(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Recreating due to idleness")


class ConnectionCreated(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Connection created")
