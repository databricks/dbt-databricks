import importlib.util
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

_SCRIPT = _SCRIPTS_DIR / "regenerate_timings.py"
_spec = importlib.util.spec_from_file_location("regenerate_timings", _SCRIPT)
assert _spec is not None and _spec.loader is not None
regen = importlib.util.module_from_spec(_spec)
sys.modules["regenerate_timings"] = regen
_spec.loader.exec_module(regen)


class TestClassnameToFile:
    def test_class_based_test(self):
        assert (
            regen.classname_to_file("tests.functional.adapter.grants.test_grants.TestModelGrants")
            == "tests/functional/adapter/grants/test_grants.py"
        )

    def test_module_level_test_no_class(self):
        assert regen.classname_to_file("tests.unit.test_x") == "tests/unit/test_x.py"

    def test_last_test_segment_wins(self):
        assert (
            regen.classname_to_file("tests.test_pkg.test_mod.TestThing")
            == "tests/test_pkg/test_mod.py"
        )

    def test_no_test_segment_falls_back_to_dotted_path(self):
        assert regen.classname_to_file("tests.helpers.util") == "tests/helpers/util.py"


class TestAggregatePerFile:
    def test_sums_time_across_testcases(self, tmp_path):
        xml = tmp_path / "junit.xml"
        xml.write_text(
            """<testsuite>
              <testcase classname="tests.unit.test_a.TestA" name="t1" time="1.5"/>
              <testcase classname="tests.unit.test_a.TestA" name="t2" time="2.0"/>
              <testcase classname="tests.unit.test_b.TestB" name="t3" time="4.0"/>
            </testsuite>"""
        )
        result = regen.aggregate_per_file([xml])
        assert result == {"tests/unit/test_a.py": 3.5, "tests/unit/test_b.py": 4.0}

    def test_missing_time_attr_treated_as_zero(self, tmp_path):
        xml = tmp_path / "junit.xml"
        xml.write_text(
            """<testsuite>
              <testcase classname="tests.unit.test_a.TestA" name="t1"/>
            </testsuite>"""
        )
        assert regen.aggregate_per_file([xml]) == {"tests/unit/test_a.py": 0.0}


class TestMedianAcrossRuns:
    def test_median_of_common_file(self):
        per_run = [
            {"p": {"a.py": 10.0}},
            {"p": {"a.py": 20.0}},
            {"p": {"a.py": 30.0}},
        ]
        assert regen.median_across_runs(per_run) == {"p": {"a.py": 20.0}}

    def test_file_in_some_runs_medianed_over_present_runs_only(self):
        per_run = [{"p": {"a.py": 10.0}}, {"p": {}}, {"p": {"a.py": 20.0}}]
        assert regen.median_across_runs(per_run) == {"p": {"a.py": 15.0}}


class TestMergeTimings:
    def test_new_file_is_added(self):
        old = {"p": {"a.py": 100.0}}
        fresh = {"p": {"a.py": 100.0, "b.py": 50.0}}
        merged, changes = regen.merge_timings(old, fresh)
        assert merged["p"] == {"a.py": 100.0, "b.py": 50.0}
        assert len(changes) == 1
        c = changes[0]
        assert (c.profile, c.file, c.old, c.new) == ("p", "b.py", None, 50.0)

    def test_small_move_is_ignored(self):
        # 5% move (< 10% threshold) keeps the old value; no change recorded.
        old = {"p": {"a.py": 100.0}}
        fresh = {"p": {"a.py": 105.0}}
        merged, changes = regen.merge_timings(old, fresh)
        assert merged["p"] == {"a.py": 100.0}
        assert changes == []

    def test_large_move_updates_value(self):
        old = {"p": {"a.py": 100.0}}
        fresh = {"p": {"a.py": 130.0}}
        merged, changes = regen.merge_timings(old, fresh)
        assert merged["p"] == {"a.py": 130.0}
        assert changes[0].delta_pct == 30.0

    def test_file_absent_from_fresh_keeps_old_value(self):
        old = {"p": {"a.py": 100.0, "b.py": 20.0}}
        fresh = {"p": {"a.py": 100.0}}  # b.py not measured this round
        merged, changes = regen.merge_timings(old, fresh)
        assert merged["p"] == {"a.py": 100.0, "b.py": 20.0}
        assert changes == []

    def test_threshold_is_configurable(self):
        old = {"p": {"a.py": 100.0}}
        fresh = {"p": {"a.py": 105.0}}
        merged, changes = regen.merge_timings(old, fresh, update_threshold_pct=1.0)
        assert merged["p"] == {"a.py": 105.0}
        assert changes


class TestManualReviewChanges:
    def test_flags_only_moves_over_60_pct(self):
        changes = [
            regen.FileChange(profile="p", file="a.py", old=100.0, new=130.0, delta_pct=30.0),
            regen.FileChange(profile="p", file="b.py", old=100.0, new=200.0, delta_pct=100.0),
        ]
        flagged = regen.manual_review_changes(changes)
        assert [c.file for c in flagged] == ["b.py"]

    def test_new_files_never_flagged(self):
        # A newly added file has delta_pct 0.0 -> never manual review.
        changes = [regen.FileChange(profile="p", file="new.py", new=500.0)]
        assert regen.manual_review_changes(changes) == []
