import pytest
from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptyInlineSourceRef
from dbt.tests.util import run_dbt


class TestDatabricksEmpty(BaseTestEmpty):
    pass


class TestDatabricksEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


_my_model_sql = "select 1 as id"

_my_model_unioned_sql = """
with unioned as (
    {{ dbt_utils.union_relations(relations=[ref("my_model")]) }}
)
select * from unioned
"""


class TestDatabricksEmptyWithUnionRelations:
    """`--empty` must not break dbt_utils.union_relations introspection."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _my_model_sql,
            "my_model_unioned.sql": _my_model_unioned_sql,
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"package": "dbt-labs/dbt_utils", "version": "1.3.0"}]}

    def test_union_relations_succeeds(self, project):
        run_dbt(["deps"])
        results = run_dbt(["run", "-s", "+my_model_unioned", "--empty"])
        assert len(results) == 2
        for r in results:
            assert r.status == "success", f"{r.node.name} failed: {r.message}"
