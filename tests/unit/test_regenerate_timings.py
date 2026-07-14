"""Unit tests for scripts/regenerate_timings.py drift computation.

The script lives in scripts/ (not the importable package), so load it by path.
The drift functions are what the weekly refresh workflow gates its PR on, so
they carry the shard-count-independence guarantee and are worth testing directly.
"""

import importlib.util
import math
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
# regenerate_timings.py imports its sibling `refresh_timings` module, which only
# resolves when scripts/ is importable (it is when run as a script).
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

_SCRIPT = _SCRIPTS_DIR / "regenerate_timings.py"
_spec = importlib.util.spec_from_file_location("regenerate_timings", _SCRIPT)
assert _spec is not None and _spec.loader is not None
regen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(regen)


class TestTotalWallByProfile:
    def test_sums_each_profile(self):
        doc = {
            "databricks_cluster": {"a.py": 10.0, "b.py": 20.0},
            "databricks_uc_cluster": {"c.py": 5.5},
        }
        assert regen.total_wall_by_profile(doc) == {
            "databricks_cluster": 30.0,
            "databricks_uc_cluster": 5.5,
        }

    def test_empty_profile_is_zero(self):
        assert regen.total_wall_by_profile({"p": {}}) == {"p": 0.0}

    def test_empty_doc(self):
        assert regen.total_wall_by_profile({}) == {}


class TestDriftByProfile:
    def test_absolute_percent_change_both_directions(self):
        old = {"p": {"a.py": 100.0}}
        # +10% (increase) and a separate profile with -20% (decrease) both report
        # positive magnitude — drift is direction-agnostic.
        new_up = {"p": {"a.py": 110.0}}
        new_down = {"p": {"a.py": 80.0}}
        assert regen.drift_by_profile(old, new_up)["p"] == 10.0
        assert regen.drift_by_profile(old, new_down)["p"] == 20.0

    def test_is_independent_of_file_layout_only_totals_matter(self):
        # Same total wall, redistributed across files -> zero drift. This is the
        # property that lets the decision ignore shard counts entirely.
        old = {"p": {"a.py": 60.0, "b.py": 40.0}}
        new = {"p": {"a.py": 30.0, "b.py": 30.0, "c.py": 40.0}}
        assert regen.drift_by_profile(old, new)["p"] == 0.0

    def test_new_profile_reports_inf(self):
        old = {"p": {"a.py": 10.0}}
        new = {"p": {"a.py": 10.0}, "q": {"b.py": 5.0}}
        drift = regen.drift_by_profile(old, new)
        assert drift["p"] == 0.0
        assert math.isinf(drift["q"])

    def test_zero_old_and_zero_new_is_zero_not_inf(self):
        assert regen.drift_by_profile({"p": {}}, {"p": {}})["p"] == 0.0

    def test_profile_dropped_from_new_reports_full_drop(self):
        old = {"p": {"a.py": 100.0}}
        new = {}
        assert regen.drift_by_profile(old, new)["p"] == 100.0


class TestMaxDriftPct:
    def test_returns_worst_profile(self):
        assert regen.max_drift_pct({"p": 5.0, "q": 42.0, "r": 1.0}) == 42.0

    def test_empty_is_zero(self):
        assert regen.max_drift_pct({}) == 0.0

    def test_inf_dominates(self):
        assert math.isinf(regen.max_drift_pct({"p": 9.0, "q": float("inf")}))


class TestFormatDriftSummary:
    def test_includes_each_profile_and_max_line(self):
        old = {"p": {"a.py": 600.0}, "q": {"b.py": 300.0}}
        new = {"p": {"a.py": 660.0}, "q": {"b.py": 300.0}}
        drift = regen.drift_by_profile(old, new)
        summary = regen.format_drift_summary(old, new, drift)
        assert "`p`" in summary
        assert "`q`" in summary
        assert "10.0%" in summary  # p drifted 10%
        assert "Max per-profile total-wall drift: 10.0%" in summary

    def test_new_profile_renders_na_not_inf(self):
        old = {"p": {"a.py": 10.0}}
        new = {"p": {"a.py": 10.0}, "q": {"b.py": 5.0}}
        drift = regen.drift_by_profile(old, new)
        summary = regen.format_drift_summary(old, new, drift)
        assert "n/a (new profile)" in summary
        assert "inf" not in summary
