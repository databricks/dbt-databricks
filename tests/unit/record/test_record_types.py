"""Unit tests for the record/replay instrumentation on DatabricksAdapter.

Each test drives a real, decorated adapter method under a RECORD-mode recorder
and asserts on the record that lands in the recorder. Every method is tested the
same way:

  1. A recorder is installed on the invocation context (the decorator reads it
     from there; without it the call would not be recorded).
  2. The method's Databricks interaction is mocked, so the body runs offline.
  3. The real decorated method is called with real arguments.
  4. The captured record is serialized via Record.to_dict() and its "params"
     and "result" dicts are asserted against the expected shape. This is the
     exact dict the recorder would write to a recording.
"""

import json
from decimal import Decimal
from multiprocessing import get_context
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from dbt_common.context import get_invocation_context, set_invocation_context
from dbt_common.record import Recorder, RecorderMode

from dbt.adapters.databricks.impl import (
    DatabricksAdapter,
    IncrementalTableAPI,
    MaterializedViewAPI,
    MetricViewAPI,
    StreamingTableAPI,
    ViewAPI,
)
from dbt.adapters.databricks.relation import DatabricksRelation, DatabricksRelationType
from dbt.adapters.databricks.relation_configs.column_comments import ColumnCommentsConfig
from dbt.adapters.databricks.relation_configs.column_mask import ColumnMaskConfig
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.constraints import (
    CheckConstraint,
    ConstraintsConfig,
    ConstraintType,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from dbt.adapters.databricks.relation_configs.incremental import IncrementalTableConfig
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringConfig
from dbt.adapters.databricks.relation_configs.materialized_view import MaterializedViewConfig
from dbt.adapters.databricks.relation_configs.metric_view import (
    MetricViewConfig,
    MetricViewQueryConfig,
)
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.query import QueryConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.row_filter import RowFilterConfig
from dbt.adapters.databricks.relation_configs.streaming_table import StreamingTableConfig
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from dbt.adapters.databricks.relation_configs.view import ViewConfig
from tests.unit.utils import config_from_parts_or_dicts

THREAD_NAME = "test_thread"
FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    """Load a recorded fixture captured from a real dbt-databricks run."""
    return json.loads((FIXTURES / name).read_text())


def get_record(recorder: Recorder, record_name: str) -> dict:
    records = recorder._records_by_type[record_name]
    assert len(records) == 1
    return records[0].to_dict()


def sort_constraints(result_config: dict) -> dict:
    """Sort the set-derived lists in a constraints component in place.

    These are serialized from Python sets, so their list order is not
    deterministic across runs; sorting makes exact-dict assertions stable.
    """
    c = result_config.get("constraints")
    if c:
        c["set_non_nulls"] = sorted(c["set_non_nulls"])
        c["unset_non_nulls"] = sorted(c["unset_non_nulls"])
        c["set_constraints"] = sorted(c["set_constraints"], key=lambda x: x["name"])
        c["unset_constraints"] = sorted(c["unset_constraints"], key=lambda x: x["name"])
    return result_config


class TestRecordTypes:
    @pytest.fixture
    def adapter(self) -> DatabricksAdapter:
        project_cfg = {
            "name": "test_project",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "databricks",
                    "catalog": "main",
                    "schema": "analytics",
                    "host": "test.databricks.com",
                    "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
                    "token": "dapi" + "X" * 32,
                }
            },
            "target": "test",
        }
        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        return DatabricksAdapter(config, get_context("spawn"))

    @pytest.fixture
    def recorder(self) -> Recorder:
        # Install an in-memory RECORD recorder on the invocation context. The
        # known name makes the decorator-injected thread_id deterministic.
        set_invocation_context({})
        ctx = get_invocation_context()
        ctx.name = THREAD_NAME
        recorder = Recorder(RecorderMode.RECORD, None, in_memory=True)
        ctx.recorder = recorder
        return recorder

    def test_record_is_cluster(self, adapter: DatabricksAdapter, recorder: Recorder) -> None:
        adapter.connections.is_cluster = Mock(return_value=True)

        adapter.is_cluster()

        record = get_record(recorder, "AdapterIsClusterRecord")
        assert record["params"] == {"thread_id": THREAD_NAME}
        assert record["result"] == {"return_val": True}

    def test_record_has_dbr_capability(
        self, adapter: DatabricksAdapter, recorder: Recorder
    ) -> None:
        adapter.has_capability = Mock(return_value=True)

        adapter.has_dbr_capability("insert_by_name")

        record = get_record(recorder, "AdapterHasDbrCapabilityRecord")
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "capability_name": "insert_by_name",
        }
        assert record["result"] == {"return_val": True}

    def test_record_add_query(self, adapter: DatabricksAdapter, recorder: Recorder) -> None:
        adapter.connections.add_query = Mock()

        sql = "insert overwrite `db`.`sch`.`s_data` values\n          (%s,%s),(%s,%s)"
        adapter.add_query(
            sql,
            auto_begin=True,
            bindings=[Decimal("1"), "a", Decimal("2"), "b"],
            abridge_sql_log=True,
            close_cursor=True,
        )

        # The add_query result just gets discarded from recordings.
        record = get_record(recorder, "DatabricksAdapterAddQueryRecord")
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "sql": sql,
            "auto_begin": True,
            "bindings": [1.0, "a", 2.0, "b"],
            "abridge_sql_log": True,
            "close_cursor": True,
        }

    def test_record_is_uniform(self, adapter: DatabricksAdapter, recorder: Recorder) -> None:
        config_model = load_fixture("is_uniform_config_model.json")
        adapter.build_catalog_relation = Mock(return_value=Mock(table_format="default"))
        config = Mock()
        config.model.to_dict.return_value = config_model
        config.get.side_effect = lambda key, default=None: {
            "materialized": config_model["config"]["materialized"],
        }.get(key, default)

        adapter.is_uniform(config)

        record = get_record(recorder, "DatabricksAdapterIsUniformRecord")
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "config_model": config_model,
            "materialized": "table",
        }
        assert record["result"] == {"return_val": False}

    # get_relation_config dispatches on relation.type.
    #
    # Each test below builds a real relation of one type plus a fully-populated
    # config, passes that config as model_config, and mocks the matching API to
    # return the same config as the result.
    #
    # We use patch.object to override the internal call to the *API method.

    def test_record_get_relation_config_materialized_view(
        self, adapter: DatabricksAdapter, recorder: Recorder
    ) -> None:
        relation = DatabricksRelation.create(
            identifier="my_mv",
            schema="analytics",
            database="main",
            type=DatabricksRelationType.MaterializedView,
            metadata={"Provider": "delta"},
        )
        config = MaterializedViewConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["region", "dt"]),
                "liquid_clustering": LiquidClusteringConfig(
                    auto_cluster=True, cluster_by=["id", "region"]
                ),
                "comment": CommentConfig(comment="rich mv", persist=True),
                "tblproperties": TblPropertiesConfig(
                    tblproperties={"delta.autoOptimize.optimizeWrite": "true", "team": "data"},
                    pipeline_id="pid-1",
                ),
                "refresh": RefreshConfig(
                    cron="0 5 * * *", time_zone_value="America/Los_Angeles", is_altered=True
                ),
                "query": QueryConfig(query="select * from src"),
                "tags": TagsConfig(set_tags={"team": "data", "tier": "gold"}),
                "row_filter": RowFilterConfig(
                    function="cat.sch.filt", columns=("region", "tenant"), is_change=True
                ),
            }
        )

        expected_config = {
            "partition_by": {"partition_by": ["region", "dt"]},
            "liquid_clustering": {"auto_cluster": True, "cluster_by": ["id", "region"]},
            "comment": {"comment": "rich mv", "persist": True},
            "tblproperties": {
                "tblproperties": {"delta.autoOptimize.optimizeWrite": "true", "team": "data"},
                "pipeline_id": "pid-1",
            },
            "refresh": {
                "cron": "0 5 * * *",
                "time_zone_value": "America/Los_Angeles",
                "every": None,
                "on_update": False,
                "at_most_every": None,
                "is_altered": True,
            },
            "query": {"query": "select * from src"},
            "tags": {"set_tags": {"team": "data", "tier": "gold"}},
            "row_filter": {
                "function": "cat.sch.filt",
                "columns": ["region", "tenant"],
                "should_unset": False,
                "is_change": True,
            },
        }

        with patch.object(MaterializedViewAPI, "get_from_relation", return_value=config):
            adapter.get_relation_config(relation, config)

        record = get_record(recorder, "DatabricksAdapterGetRelationConfigRecord")
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "relation": relation.to_dict(omit_none=True),
            "model_config": {"config": expected_config},
        }
        assert record["result"] == {"return_val": {"config": expected_config}}

    def test_record_get_relation_config_streaming_table(
        self, adapter: DatabricksAdapter, recorder: Recorder
    ) -> None:
        relation = DatabricksRelation.create(
            identifier="my_st",
            schema="analytics",
            database="main",
            type=DatabricksRelationType.StreamingTable,
            metadata={"Provider": "delta"},
        )
        config = StreamingTableConfig(
            config={
                "partition_by": PartitionedByConfig(partition_by=["region"]),
                "liquid_clustering": LiquidClusteringConfig(auto_cluster=False, cluster_by=["id"]),
                "comment": CommentConfig(comment="rich st", persist=True),
                "tblproperties": TblPropertiesConfig(
                    tblproperties={"team": "data"}, pipeline_id="pid-2"
                ),
                "refresh": RefreshConfig(every="2 HOURS"),
                "tags": TagsConfig(set_tags={"team": "data"}),
                "query": QueryConfig(query="select * from stream(src)"),
                "row_filter": RowFilterConfig(function="cat.sch.filt", columns=("region",)),
            }
        )

        expected_config = {
            "partition_by": {"partition_by": ["region"]},
            "liquid_clustering": {"auto_cluster": False, "cluster_by": ["id"]},
            "comment": {"comment": "rich st", "persist": True},
            "tblproperties": {"tblproperties": {"team": "data"}, "pipeline_id": "pid-2"},
            "refresh": {
                "cron": None,
                "time_zone_value": None,
                "every": "2 HOURS",
                "on_update": False,
                "at_most_every": None,
                "is_altered": False,
            },
            "tags": {"set_tags": {"team": "data"}},
            "query": {"query": "select * from stream(src)"},
            "row_filter": {
                "function": "cat.sch.filt",
                "columns": ["region"],
                "should_unset": False,
                "is_change": False,
            },
        }

        with patch.object(StreamingTableAPI, "get_from_relation", return_value=config):
            adapter.get_relation_config(relation, config)

        record = get_record(recorder, "DatabricksAdapterGetRelationConfigRecord")
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "relation": relation.to_dict(omit_none=True),
            "model_config": {"config": expected_config},
        }
        assert record["result"] == {"return_val": {"config": expected_config}}

    def test_record_get_relation_config_table(
        self, adapter: DatabricksAdapter, recorder: Recorder
    ) -> None:
        relation = DatabricksRelation.create(
            identifier="my_table",
            schema="analytics",
            database="main",
            type=DatabricksRelationType.Table,
            is_delta=True,
            metadata={"Provider": "delta"},
        )
        config = IncrementalTableConfig(
            config={
                "comment": CommentConfig(comment="rich inc", persist=True),
                "column_comments": ColumnCommentsConfig(
                    comments={"id": "the id", "email": "user email"}, persist=True
                ),
                "column_masks": ColumnMaskConfig(
                    set_column_masks={
                        "ssn": {"function": "cat.sch.mask_ssn", "using_columns": "region"}
                    },
                    unset_column_masks=["old_col"],
                ),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"email": {"pii": "true"}, "id": {"pk": "true"}}
                ),
                "constraints": ConstraintsConfig(
                    set_non_nulls={"id", "email"},
                    unset_non_nulls={"legacy"},
                    set_constraints={
                        CheckConstraint(
                            type=ConstraintType.check,
                            name="chk_len",
                            expression="length(name) >= 1",
                        ),
                        PrimaryKeyConstraint(
                            type=ConstraintType.primary_key, name="pk_id", columns=["id"]
                        ),
                        ForeignKeyConstraint(
                            type=ConstraintType.foreign_key,
                            name="fk_org",
                            columns=["org_id"],
                            to="cat.sch.orgs",
                            to_columns=["id"],
                        ),
                    },
                    unset_constraints={
                        CheckConstraint(
                            type=ConstraintType.check, name="old_chk", expression="x > 0"
                        )
                    },
                ),
                "row_filter": RowFilterConfig(function="cat.sch.filt", columns=("region",)),
                "tags": TagsConfig(set_tags={"team": "data"}),
                "tblproperties": TblPropertiesConfig(tblproperties={"delta.appendOnly": "false"}),
                "liquid_clustering": LiquidClusteringConfig(auto_cluster=False, cluster_by=["id"]),
            }
        )

        expected_config = {
            "comment": {"comment": "rich inc", "persist": True},
            "column_comments": {
                "comments": {"id": "the id", "email": "user email"},
                "persist": True,
            },
            "column_masks": {
                "set_column_masks": {
                    "ssn": {"function": "cat.sch.mask_ssn", "using_columns": "region"}
                },
                "unset_column_masks": ["old_col"],
            },
            "column_tags": {"set_column_tags": {"email": {"pii": "true"}, "id": {"pk": "true"}}},
            "constraints": {
                "set_non_nulls": ["email", "id"],
                "unset_non_nulls": ["legacy"],
                "set_constraints": [
                    {
                        "type": "check",
                        "name": "chk_len",
                        "expression": "length(name) >= 1",
                        "warn_unenforced": True,
                        "warn_unsupported": True,
                        "to": None,
                        "to_columns": [],
                        "columns": [],
                    },
                    {
                        "type": "foreign_key",
                        "name": "fk_org",
                        "expression": None,
                        "warn_unenforced": True,
                        "warn_unsupported": True,
                        "to": "cat.sch.orgs",
                        "to_columns": ["id"],
                        "columns": ["org_id"],
                    },
                    {
                        "type": "primary_key",
                        "name": "pk_id",
                        "expression": None,
                        "warn_unenforced": True,
                        "warn_unsupported": True,
                        "to": None,
                        "to_columns": [],
                        "columns": ["id"],
                    },
                ],
                "unset_constraints": [
                    {
                        "type": "check",
                        "name": "old_chk",
                        "expression": "x > 0",
                        "warn_unenforced": True,
                        "warn_unsupported": True,
                        "to": None,
                        "to_columns": [],
                        "columns": [],
                    }
                ],
            },
            "row_filter": {
                "function": "cat.sch.filt",
                "columns": ["region"],
                "should_unset": False,
                "is_change": False,
            },
            "tags": {"set_tags": {"team": "data"}},
            "tblproperties": {"tblproperties": {"delta.appendOnly": "false"}, "pipeline_id": None},
            "liquid_clustering": {"auto_cluster": False, "cluster_by": ["id"]},
        }

        with patch.object(IncrementalTableAPI, "get_from_relation", return_value=config):
            adapter.get_relation_config(relation, config)

        record = get_record(recorder, "DatabricksAdapterGetRelationConfigRecord")
        # Sort the set-derived constraint lists (serialized from sets) for determinism.
        sort_constraints(record["params"]["model_config"]["config"])
        sort_constraints(record["result"]["return_val"]["config"])
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "relation": relation.to_dict(omit_none=True),
            "model_config": {"config": expected_config},
        }
        assert record["result"] == {"return_val": {"config": expected_config}}

    def test_record_get_relation_config_view(
        self, adapter: DatabricksAdapter, recorder: Recorder
    ) -> None:
        relation = DatabricksRelation.create(
            identifier="my_view",
            schema="analytics",
            database="main",
            type=DatabricksRelationType.View,
            metadata={"Provider": "delta"},
        )
        config = ViewConfig(
            config={
                "tags": TagsConfig(set_tags={"team": "data", "tier": "silver"}),
                "tblproperties": TblPropertiesConfig(tblproperties={"team": "data"}),
                "query": QueryConfig(query="select * from src"),
                "comment": CommentConfig(comment="rich view", persist=True),
                "column_comments": ColumnCommentsConfig(comments={"id": "the id"}, persist=True),
                "column_tags": ColumnTagsConfig(set_column_tags={"id": {"pii": "false"}}),
            }
        )

        expected_config = {
            "tags": {"set_tags": {"team": "data", "tier": "silver"}},
            "tblproperties": {"tblproperties": {"team": "data"}, "pipeline_id": None},
            "query": {"query": "select * from src"},
            "comment": {"comment": "rich view", "persist": True},
            "column_comments": {"comments": {"id": "the id"}, "persist": True},
            "column_tags": {"set_column_tags": {"id": {"pii": "false"}}},
        }

        with patch.object(ViewAPI, "get_from_relation", return_value=config):
            adapter.get_relation_config(relation, config)

        record = get_record(recorder, "DatabricksAdapterGetRelationConfigRecord")
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "relation": relation.to_dict(omit_none=True),
            "model_config": {"config": expected_config},
        }
        assert record["result"] == {"return_val": {"config": expected_config}}

    def test_record_get_relation_config_metric_view(
        self, adapter: DatabricksAdapter, recorder: Recorder
    ) -> None:
        relation = DatabricksRelation.create(
            identifier="my_metric_view",
            schema="analytics",
            database="main",
            type=DatabricksRelationType.MetricView,
        )
        config = MetricViewConfig(
            config={
                "tags": TagsConfig(set_tags={"team": "metrics"}),
                "tblproperties": TblPropertiesConfig(tblproperties={"team": "data"}),
                "query": MetricViewQueryConfig(
                    query="version: 0.1\nsource: orders\nmeasures:\n  - name: n\n    expr: count(1)"
                ),
            }
        )

        expected_config = {
            "tags": {"set_tags": {"team": "metrics"}},
            "tblproperties": {"tblproperties": {"team": "data"}, "pipeline_id": None},
            "query": {
                "query": "version: 0.1\nsource: orders\nmeasures:\n  - name: n\n    expr: count(1)"
            },
        }

        with patch.object(MetricViewAPI, "get_from_relation", return_value=config):
            adapter.get_relation_config(relation, config)

        record = get_record(recorder, "DatabricksAdapterGetRelationConfigRecord")
        assert record["params"] == {
            "thread_id": THREAD_NAME,
            "relation": relation.to_dict(omit_none=True),
            "model_config": {"config": expected_config},
        }
        assert record["result"] == {"return_val": {"config": expected_config}}
