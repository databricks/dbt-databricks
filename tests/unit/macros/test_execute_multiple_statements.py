import pytest
from jinja2 import Environment, FileSystemLoader


@pytest.fixture
def stored():
    return []


@pytest.fixture
def execute_multiple_statements(stored):
    def statement(name=None, fetch_result=False, auto_begin=True, caller=None):
        sql = caller()
        if name is not None:
            stored.append((name, sql.strip()))
        return ""

    env = Environment(
        loader=FileSystemLoader("dbt/include/databricks/macros/etc"),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("statement.sql", globals={"statement": statement})
    return template.module.execute_multiple_statements


def test_delete_insert_stores_insert_as_main(execute_multiple_statements, stored):
    delete_sql = "delete from t where id in (select id from s)"
    insert_sql = "insert into t select * from s"

    execute_multiple_statements([delete_sql, insert_sql])

    assert stored == [("main", insert_sql)]


def test_only_last_statement_is_stored_for_longer_lists(execute_multiple_statements, stored):
    execute_multiple_statements(["prep 1", "prep 2", "write"])

    assert stored == [("main", "write")]


def test_string_statement_is_stored_as_main(execute_multiple_statements, stored):
    execute_multiple_statements("insert into t select 1")

    assert stored == [("main", "insert into t select 1")]
