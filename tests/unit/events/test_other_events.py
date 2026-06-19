from databricks.sql.exc import Error

from dbt.adapters.databricks.events.other_events import QueryError


class TestQueryError:
    def test_query_error__formats_sql_and_exception(self):
        result = str(QueryError("select 1", Exception("boom")))
        assert "select 1" in result
        assert result.endswith("boom")

    def test_query_error__with_pysql_error_appends_properties(self):
        e = Error("bad query", {"sqlstate": "42000", "operation": "select"})
        event = QueryError("select 1", e)
        assert (
            str(event) == "Exception while trying to execute query\nselect 1\n: bad query\n"
            "Error properties: operation=select, sqlstate=42000"
        )
