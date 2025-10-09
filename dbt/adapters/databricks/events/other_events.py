from dbt.adapters.databricks.events.base import SQLErrorEvent


class QueryError(SQLErrorEvent):
    def __init__(self, log_sql: str, exception: Exception):
        super().__init__(exception, f"Exception while trying to execute query\n{log_sql}\n")
