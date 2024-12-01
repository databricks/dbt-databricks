from abc import ABC
from uuid import UUID

from databricks.sql.client import Cursor

from dbt.adapters.databricks.events.base import SQLErrorEvent


class CursorEvent(ABC):
    def __init__(self, cursor: Cursor, message: str):
        self.message = message
        self.session_id = "Unknown"
        self.command_id = "Unknown"
        if cursor:
            if cursor.connection:
                self.session_id = cursor.connection.get_session_id_hex()
            if (
                cursor.active_result_set
                and cursor.active_result_set.command_id
                and cursor.active_result_set.command_id.operationId
            ):
                self.command_id = (
                    str(UUID(bytes=cursor.active_result_set.command_id.operationId.guid))
                    or "Unknown"
                )

    def __str__(self) -> str:
        return (
            f"Cursor(session-id={self.session_id}, command-id={self.command_id}) - {self.message}"
        )


class CursorCloseError(CursorEvent):
    def __init__(self, cursor: Cursor, exception: Exception):
        super().__init__(
            cursor, str(SQLErrorEvent(exception, "Exception while trying to close cursor"))
        )


class CursorCancelError(CursorEvent):
    def __init__(self, cursor: Cursor, exception: Exception):
        super().__init__(
            cursor, str(SQLErrorEvent(exception, "Exception while trying to cancel cursor"))
        )


class CursorCreate(CursorEvent):
    def __init__(self, cursor: Cursor):
        super().__init__(cursor, "Created cursor")


class CursorClose(CursorEvent):
    def __init__(self, cursor: Cursor):
        super().__init__(cursor, "Closing cursor")


class CursorCancel(CursorEvent):
    def __init__(self, cursor: Cursor):
        super().__init__(cursor, "Cancelling cursor")
