from dbt.tests import util
from tests.functional.adapter.incremental import fixtures

import pytest


class TestIncrementalPersistDocs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.no_comment_schema,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_adding_comments(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.comment_schema, "models", "schema.yml")
        _, out = util.run_dbt_and_capture(["--debug", "run"])
        assert "comment 'This is the id column'" in out
        assert "comment 'This is the msg column'" in out
        assert "comment ''" not in out
