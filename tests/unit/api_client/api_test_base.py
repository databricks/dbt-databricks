from typing import Any
from typing import Callable

import pytest
from dbt_common.exceptions import DbtRuntimeError
from mock import Mock


class ApiTestBase:
    @pytest.fixture
    def session(self):
        return Mock()

    @pytest.fixture
    def host(self):
        return "host"

    def assert_non_200_raises_error(self, operation: Callable[[], Any], session: Mock):
        session.post.return_value.status_code = 500
        with pytest.raises(DbtRuntimeError):
            operation()
