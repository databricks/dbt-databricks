from dbt.tests.adapter.basic import expected_catalog
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences
from dbt.tests.util import AnyString

import pytest

from tests.functional.adapter.basic.typing import AnyLongType, StatsLikeDict


class TestDatabricksDocsGenerate(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_catalog.base_expected_catalog(
            project,
            role=AnyString(),
            id_type=AnyLongType(),
            text_type="string",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=StatsLikeDict(),
        )


class TestDatabricksDocsGenReferences(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return self.expected_references_catalog(
            project,
            role=AnyString(),
            id_type=AnyLongType(),
            text_type="string",
            time_type="timestamp",
            bigint_type=AnyLongType(),
            view_type="view",
            table_type="table",
            model_stats=StatsLikeDict(),
        )

    # Temporary until upstream fixes to allow 0-based indexing
    def expected_references_catalog(
        self,
        project,
        role,
        id_type,
        text_type,
        time_type,
        view_type,
        table_type,
        model_stats,
        bigint_type=None,
    ):
        seed_stats = model_stats
        view_summary_stats = model_stats

        model_database = project.database
        my_schema_name = project.test_schema

        summary_columns = {
            "first_name": {
                "name": "first_name",
                "index": 0,
                "type": text_type,
                "comment": None,
            },
            "ct": {
                "name": "ct",
                "index": 1,
                "type": bigint_type,
                "comment": None,
            },
        }

        seed_columns = {
            "id": {
                "name": "id",
                "index": 0,
                "type": id_type,
                "comment": None,
            },
            "first_name": {
                "name": "first_name",
                "index": 1,
                "type": text_type,
                "comment": None,
            },
            "email": {
                "name": "email",
                "index": 2,
                "type": text_type,
                "comment": None,
            },
            "ip_address": {
                "name": "ip_address",
                "index": 3,
                "type": text_type,
                "comment": None,
            },
            "updated_at": {
                "name": "updated_at",
                "index": 4,
                "type": time_type,
                "comment": None,
            },
        }
        return {
            "nodes": {
                "seed.test.seed": {
                    "unique_id": "seed.test.seed",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": project.database,
                        "name": "seed",
                        "type": table_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": seed_stats,
                    "columns": seed_columns,
                },
                "model.test.ephemeral_summary": {
                    "unique_id": "model.test.ephemeral_summary",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": model_database,
                        "name": "ephemeral_summary",
                        "type": table_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": model_stats,
                    "columns": summary_columns,
                },
                "model.test.view_summary": {
                    "unique_id": "model.test.view_summary",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": model_database,
                        "name": "view_summary",
                        "type": view_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": view_summary_stats,
                    "columns": summary_columns,
                },
            },
            "sources": {
                "source.test.my_source.my_table": {
                    "unique_id": "source.test.my_source.my_table",
                    "metadata": {
                        "schema": my_schema_name,
                        "database": project.database,
                        "name": "seed",
                        "type": table_type,
                        "comment": None,
                        "owner": role,
                    },
                    "stats": seed_stats,
                    "columns": seed_columns,
                },
            },
        }
