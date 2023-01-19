from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


class TestQueryCommentsDatabricks(BaseQueryComments):
    pass


class TestMacroQueryCommentsDatabricks(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsDatabricks(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsDatabricks(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsDatabricks(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsDatabricks(BaseEmptyQueryComments):
    pass
