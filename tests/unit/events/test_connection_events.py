from unittest.mock import Mock

from dbt.adapters.databricks.events.connection_events import (
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
