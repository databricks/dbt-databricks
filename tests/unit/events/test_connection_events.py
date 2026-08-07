from unittest.mock import Mock

from dbt.adapters.databricks.events.connection_events import (
    ConnectionCreate,
    ConnectionCreateError,
    ConnectionEvent,
)


class ConnectionTestEvent(ConnectionEvent):
    def __init__(self, connection):
        super().__init__(connection, "This is a test")


class TestConnectionEvents:
    def test_cursor_event__no_connection(self):
        event = ConnectionTestEvent(None)
        assert str(event) == "Connection(session-id=Unknown) - This is a test"

    def test_connection_event__no_id(self):
        mock = Mock()
        mock.get_session_id_hex.return_value = None
        event = ConnectionTestEvent(mock)
        assert str(event) == "Connection(session-id=Unknown) - This is a test"

    def test_connection_event__with_id(self):
        mock = Mock()
        mock.get_session_id_hex.return_value = "1234"
        event = ConnectionTestEvent(mock)
        assert str(event) == "Connection(session-id=1234) - This is a test"


class TestConnectionCreateError:
    def test_connection_create_error__formats_exception(self):
        result = str(ConnectionCreateError(Exception("nope")))
        assert "session-id=Unknown" in result
        assert "nope" in result


class TestConnectionCreate:
    def test_connection_create(self):
        assert str(ConnectionCreate("conn-1")) == "conn-1 - Creating connection"
