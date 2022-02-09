from cProfile import run
from tests.integration.base import DBTIntegrationTest, use_profile
import dbt.exceptions

import json


class TestSetTblproperties(DBTIntegrationTest):
    @property
    def schema(self):
        return "set_tblproperties"
        
    @property
    def models(self):
        return "models"

    def test_set_tblproperties(self):
        self.run_dbt(['seed'])
        self.run_dbt(['run'])
        self.run_dbt(['run'])

        self.assertTablesEqual("set_tblproperties", "expected")

        results = self.run_sql(
            'describe extended {schema}.{table}'.format(
                schema=self.unique_schema(), table='set_tblproperties'
            ),
            fetch='all'
        )

        for result in results:
            if result[0] == 'Table Properties':
                assert 'delta.autoOptimize.optimizeWrite' in result[1]
                assert 'delta.autoOptimize.autoCompact' in result[1]

    @use_profile("databricks_cluster")
    def test_set_tblproperties_databricks_cluster(self):
        self.test_set_tblproperties()

    @use_profile("databricks_sql_endpoint")
    def test_set_tblproperties_databricks_sql_endpoint(self):
        self.test_set_tblproperties()
