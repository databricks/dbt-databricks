from unittest.mock import Mock

import pytest


class ApiTestBase:
    @pytest.fixture
    def client(self):
        return Mock()
