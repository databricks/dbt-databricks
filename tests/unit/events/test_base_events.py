from databricks.sql.exc import Error

from dbt.adapters.databricks.events.base import ErrorEvent
from dbt.adapters.databricks.events.base import SQLErrorEvent


class ErrorTestEvent(ErrorEvent):
    def __init__(self, exception):
        super().__init__(exception, "This is a test")


class TestErrorEvent:
    def test_error_event__without_exception(self):
        event = ErrorTestEvent(None)
        assert str(event) == "This is a test: None"

    def test_error_event__with_exception(self):
        e = Exception("This is an exception")
        event = ErrorTestEvent(e)
        assert str(event) == "This is a test: This is an exception"


class TestSQLErrorEvent:
    def test_sql_error_event__with_exception(self):
        e = Exception("This is an exception")
        event = SQLErrorEvent(e, "This is a test")
        assert str(event) == "This is a test: This is an exception"

    def test_sql_error_event__with_pysql_error(self):
        e = Error("This is a pysql error", {"key": "value", "other": "other_value"})
        event = SQLErrorEvent(e, "This is a test")
        assert (
            str(event) == "This is a test: This is a pysql error\n"
            "Error properties: key=value, other=other_value"
        )
