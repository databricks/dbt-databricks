from mock import Mock

from dbt.adapters.databricks.events.cursor_events import CursorCloseError, CursorEvent


class CursorTestEvent(CursorEvent):
    def __init__(self, cursor):
        super().__init__(cursor, "This is a test")


class TestCursorEvents:
    def test_cursor_event__no_cursor(self):
        event = CursorTestEvent(None)
        assert str(event) == "Cursor(session-id=Unknown, command-id=Unknown) - This is a test"

    def test_cursor_event__no_ids(self):
        mock = Mock()
        mock.connection = None
        mock.active_result_set = None
        event = CursorTestEvent(mock)
        assert str(event) == "Cursor(session-id=Unknown, command-id=Unknown) - This is a test"

    def test_cursor_event__with_ids(self):
        mock = Mock()
        mock.connection.get_session_id_hex.return_value = "1234"
        mock.active_result_set.command_id.operationId.guid = (1234).to_bytes(16, "big")
        event = CursorTestEvent(mock)
        assert (
            str(event) == "Cursor(session-id=1234, command-id=00000000-0000-0000-0000-0000000004d2)"
            " - This is a test"
        )


class TestCursorCloseError:
    def test_cursor_close_error(self):
        e = Exception("This is an exception")
        event = CursorCloseError(None, e)
        assert (
            str(event) == "Cursor(session-id=Unknown, command-id=Unknown) "
            "- Exception while trying to close cursor: This is an exception"
        )
