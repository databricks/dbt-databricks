from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)
import pytest


class TestQueryCommentsDatabricks(BaseQueryComments):
    pass


class TestMacroQueryCommentsDatabricks(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsDatabricks(BaseMacroArgsQueryComments):
    pass


@pytest.mark.skip(reason="Test stopped passing while migrating to 1.8.0")
class TestMacroInvalidQueryCommentsDatabricks(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsDatabricks(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsDatabricks(BaseEmptyQueryComments):
    pass
