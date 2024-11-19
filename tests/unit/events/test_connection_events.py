from mock import Mock

from dbt.adapters.databricks.events.connection_events import (
    ConnectionAcquire,
    ConnectionCloseError,
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


class TestConnectionCloseError:
    def test_connection_close_error(self):
        e = Exception("This is an exception")
        event = ConnectionCloseError(None, e)
        assert (
            str(event) == "Connection(session-id=Unknown) "
            "- Exception while trying to close connection: This is an exception"
        )


class TestConnectionAcquire:
    def test_connection_acquire__missing_data(self):
        event = ConnectionAcquire(None, None, None, (0, 0))
        assert (
            str(event)
            == "None - Acquired connection on thread (0, 0), using default compute resource"
        )

    def test_connection_acquire__with_compute_name(self):
        event = ConnectionAcquire(None, None, "Eniac", (0, 0))
        assert (
            str(event)
            == "None - Acquired connection on thread (0, 0), using compute resource 'Eniac'"
        )

    def test_connection_acquire__with_nonmodel_node(self):
        event = ConnectionAcquire(None, Mock([]), None, (0, 0))
        assert (
            str(event)
            == "None - Acquired connection on thread (0, 0), using default compute resource"
            " for model '[Unknown]'"
        )

    def test_connection_acquire__with_everything(self):
        model = Mock()
        model.relation_name = "MyModel"
        event = ConnectionAcquire("Connection", model, "Eniac", (0, 0))
        assert (
            str(event)
            == "Connection - Acquired connection on thread (0, 0), using compute resource 'Eniac'"
            " for model 'MyModel'"
        )
