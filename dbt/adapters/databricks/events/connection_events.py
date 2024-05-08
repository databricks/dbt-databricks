from abc import ABC
from typing import Any
from typing import Optional
from typing import Tuple

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


class ConnectionCancel(ConnectionEvent):
    def __init__(self, connection: Optional[Connection]):
        super().__init__(connection, "Cancelling connection")


class ConnectionClose(ConnectionEvent):
    def __init__(self, connection: Optional[Connection]):
        super().__init__(connection, "Closing connection")


class ConnectionCancelError(ConnectionEvent):
    def __init__(self, connection: Optional[Connection], exception: Exception):
        super().__init__(
            connection, str(SQLErrorEvent(exception, "Exception while trying to cancel connection"))
        )


class ConnectionCloseError(ConnectionEvent):
    def __init__(self, connection: Optional[Connection], exception: Exception):
        super().__init__(
            connection, str(SQLErrorEvent(exception, "Exception while trying to close connection"))
        )


class ConnectionCreateError(ConnectionEvent):
    def __init__(self, connection: Optional[Connection], exception: Exception):
        super().__init__(
            connection, str(SQLErrorEvent(exception, "Exception while trying to create connection"))
        )


class ConnectionWrapperEvent(ABC):
    def __init__(self, description: str, message: str):
        self.message = message
        self.description = description

    def __str__(self) -> str:
        return f"{self.description} - {self.message}"


class ConnectionAcquire(ConnectionWrapperEvent):
    def __init__(
        self,
        description: str,
        model: Optional[Any],
        compute_name: Optional[str],
        thread_identifier: Tuple[int, int],
    ):
        message = f"Acquired connection on thread {thread_identifier}, using "
        if not compute_name:
            message += "default compute resource"
        else:
            message += f"compute resource '{compute_name}'"

        if model:
            # ResultNode *should* have relation_name attr, but we work around a core
            # issue by checking.
            relation_name = getattr(model, "relation_name", "[Unknown]")
            message += f" for model '{relation_name}'"

        super().__init__(description, message)


class ConnectionRelease(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Released connection")


class ConnectionReset(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Reset connection handle")


class ConnectionReuse(ConnectionWrapperEvent):
    def __init__(self, description: str, prior_name: str):
        super().__init__(description, f"Reusing connection previously named {prior_name}")


class ConnectionCreate(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Creating connection")


class ConnectionIdleCheck(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Checking idleness")


class ConnectionIdleClose(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Closing for idleness")


class ConnectionRetrieve(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Retrieving connection")


class ConnectionCreated(ConnectionWrapperEvent):
    def __init__(self, description: str):
        super().__init__(description, "Connection created")
