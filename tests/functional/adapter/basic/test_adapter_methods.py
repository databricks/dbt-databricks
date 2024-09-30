from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
import pytest


class TestAdapterMethod(BaseAdapterMethod):
    """Currently this exercises:
    * get_columns_in_relation
    * get_relation
    * drop_schema
    * create_schema
    """

    pass


class TestAdapterMethodWithJITMetadata(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"just_in_time_metadata": True}}
