import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import MaterializationV2Mixin, RerunSafeMixin
from tests.functional.adapter.incremental import fixtures


class SkipOnEmptySourceBase(RerunSafeMixin):
    model_config = "skip_merge_on_empty_source=true"

    @pytest.fixture(scope="class")
    def models(self):
        return {"skip_model.sql": fixtures.skip_on_empty_source_sql(self.model_config)}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("skip_model",)

    def latest_history(self, project):
        relation = util.relation_from_name(project.adapter, "skip_model")
        history = project.run_sql(f"describe history {relation}", fetch="all")
        latest = max(history, key=lambda row: row[0])
        return latest[0], latest[4]

    def ids(self, project):
        relation = util.relation_from_name(project.adapter, "skip_model")
        rows = project.run_sql(f"select id from {relation} order by id", fetch="all")
        return [row[0] for row in rows]


class TestSkipMergeOnEmptySource(SkipOnEmptySourceBase):
    def test_empty_source_skips_and_nonempty_source_merges(self, project):
        util.run_dbt(["run"])
        version, _ = self.latest_history(project)

        util.run_dbt(["run"])
        assert self.latest_history(project)[0] == version

        util.run_dbt(["run", "--vars", "max_id: 3"])
        new_version, operation = self.latest_history(project)
        assert new_version > version
        assert operation == "MERGE"
        assert self.ids(project) == [1, 2, 3]


class TestSkipMergeOnEmptySourceV2(MaterializationV2Mixin, TestSkipMergeOnEmptySource):
    pass


class SkipNotAppliedBase(SkipOnEmptySourceBase):
    expected_ids: list[int] = [1, 2]

    def test_empty_source_still_runs_strategy(self, project):
        util.run_dbt(["run"])
        version, _ = self.latest_history(project)

        util.run_dbt(["run"])
        assert self.latest_history(project)[0] > version
        assert self.ids(project) == self.expected_ids


class TestSkipMergeDisabledByDefault(SkipNotAppliedBase):
    model_config = "incremental_strategy='merge'"


class TestSkipNotAppliedToReplaceWhere(SkipNotAppliedBase):
    model_config = (
        "incremental_strategy='replace_where', incremental_predicates='id >= 2',"
        " skip_merge_on_empty_source=true"
    )
    expected_ids = [1]


class TestSkipNotAppliedToMergeWithNotMatchedBySource(SkipNotAppliedBase):
    model_config = "not_matched_by_source_action='delete', skip_merge_on_empty_source=true"
    expected_ids = []


class TestSkipNotAppliedWhenOnSchemaChangeIsNotIgnore(SkipNotAppliedBase):
    model_config = "on_schema_change='fail', skip_merge_on_empty_source=true"
