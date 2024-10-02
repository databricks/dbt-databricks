from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)
import pytest

from tests.functional.adapter.microbatch import fixtures


class TestDatabricksMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def models(self, microbatch_model_sql, input_model_sql):
        return {
            "schema.yml": fixtures.schema,
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }
