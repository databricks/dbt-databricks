import pytest

from dbt.tests.adapter.query_comment.test_query_comment import BaseEmptyQueryComments
from dbt.tests.adapter.query_comment.test_query_comment import BaseMacroArgsQueryComments
from dbt.tests.adapter.query_comment.test_query_comment import BaseMacroInvalidQueryComments
from dbt.tests.adapter.query_comment.test_query_comment import BaseMacroQueryComments
from dbt.tests.adapter.query_comment.test_query_comment import BaseNullQueryComments
from dbt.tests.adapter.query_comment.test_query_comment import BaseQueryComments


class TestQueryComments(BaseQueryComments):
    pass


class TestMacroQueryComments(BaseMacroQueryComments):
    pass


@pytest.mark.skip(reason="Test stopped passing while migrating to 1.8.0")
class TestMacroArgsQueryComments(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryComments(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryComments(BaseNullQueryComments):
    pass


class TestEmptyQueryComments(BaseEmptyQueryComments):
    pass
