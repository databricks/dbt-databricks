import logging as stdlib_logging
from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.logging import DbtCoreHandler


def _record(level, msg):
    return stdlib_logging.LogRecord(
        name="databricks.sql",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=None,
        exc_info=None,
    )


class TestDbtCoreHandler:
    @pytest.mark.parametrize(
        "level, method, msg",
        [
            (stdlib_logging.WARNING, "warning", "heads up"),
            (stdlib_logging.ERROR, "error", "bad"),
            (stdlib_logging.DEBUG, "debug", "trace"),
        ],
    )
    def test_emit__routes_record_to_matching_logger_method(self, level, method, msg):
        dbt_logger = Mock()
        handler = DbtCoreHandler(level=stdlib_logging.DEBUG, dbt_logger=dbt_logger)

        handler.emit(_record(level, msg))

        getattr(dbt_logger, method).assert_called_once_with(msg)
