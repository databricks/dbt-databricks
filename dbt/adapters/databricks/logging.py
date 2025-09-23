from logging import Handler, LogRecord, getLogger
from typing import Union

from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Databricks")


class DbtCoreHandler(Handler):
    def __init__(self, level: Union[str, int], dbt_logger: AdapterLogger):
        super().__init__(level=level)
        self.logger = dbt_logger

    def emit(self, record: LogRecord) -> None:
        # record.levelname will be debug, info, warning, error, or critical
        # these map 1-to-1 with methods of the AdapterLogger
        log_func = getattr(self.logger, record.levelname.lower())
        log_func(record.msg)


dbt_adapter_logger = AdapterLogger("databricks-sql-connector")

pysql_logger = getLogger("databricks.sql")
pysql_logger_level = GlobalState.get_connector_log_level()
pysql_logger.setLevel(pysql_logger_level)

pysql_handler = DbtCoreHandler(dbt_logger=dbt_adapter_logger, level=pysql_logger_level)
pysql_logger.addHandler(pysql_handler)
