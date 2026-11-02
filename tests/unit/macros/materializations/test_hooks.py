from unittest.mock import Mock, patch

import pytest
from dbt_common.behavior_flags import Behavior
from dbt_common.events.types import BehaviorChangeEvent
from jinja2 import Environment, FileSystemLoader

from dbt.adapters.databricks.impl import USE_NON_TRANSACTIONAL_HOOKS

ORDINARY = {"sql": "ordinary hook", "transaction": True}
OUTSIDE = {"sql": "outside hook", "transaction": False}
ORDINARY_EVENTS = [("render", "ordinary hook"), ("statement", "ordinary hook", True)]
OUTSIDE_EVENTS = [("render", "outside hook"), ("statement", "outside hook", False)]


@pytest.fixture
def fired_events():
    with patch("dbt_common.behavior_flags.fire_event") as fire_event:
        yield fire_event


def make_hooks_module(behavior, pre_hooks=(), post_hooks=()):
    events = []

    def statement(auto_begin, caller):
        events.append(("statement", caller().strip(), auto_begin))
        return ""

    def render(sql):
        events.append(("render", sql))
        return sql

    context = {
        "statement": statement,
        "render": render,
        "adapter": Mock(behavior=behavior),
        "pre_hooks": list(pre_hooks),
        "post_hooks": list(post_hooks),
    }
    env = Environment(loader=FileSystemLoader("dbt/include/databricks/macros/materializations"))
    return env.get_template("hooks.sql").make_module(context), events


def behavior(flag_enabled=None):
    overrides = {} if flag_enabled is None else {"use_non_transactional_hooks": flag_enabled}
    return Behavior([USE_NON_TRANSACTIONAL_HOOKS], overrides)


def behavior_change_events(fired_events):
    events = [c.args[0] for c in fired_events.call_args_list]
    return [event for event in events if isinstance(event, BehaviorChangeEvent)]


@pytest.mark.parametrize("flag_enabled", [None, False], ids=["default", "explicit-false"])
def test_outside_hooks_skipped_with_one_warning_per_invocation(fired_events, flag_enabled):
    hooks, events = make_hooks_module(
        behavior(flag_enabled), pre_hooks=[OUTSIDE, ORDINARY], post_hooks=[ORDINARY, OUTSIDE]
    )
    for _ in range(2):
        hooks.run_pre_hooks()
        hooks.run_post_hooks()

    assert events == ORDINARY_EVENTS * 4
    warnings = behavior_change_events(fired_events)
    assert len(warnings) == 1
    assert warnings[0].flag_name == "use_non_transactional_hooks"


@pytest.mark.parametrize("phase", ["pre", "post"])
def test_outside_hooks_run_without_commit_when_flag_enabled(fired_events, phase):
    hooks, events = make_hooks_module(behavior(True), **{f"{phase}_hooks": [ORDINARY, OUTSIDE]})
    getattr(hooks, f"run_{phase}_hooks")()

    expected = (
        OUTSIDE_EVENTS + ORDINARY_EVENTS if phase == "pre" else ORDINARY_EVENTS + OUTSIDE_EVENTS
    )
    assert events == expected
    assert behavior_change_events(fired_events) == []


def test_ordinary_hooks_only_do_not_warn(fired_events):
    hooks, events = make_hooks_module(behavior(), pre_hooks=[ORDINARY], post_hooks=[ORDINARY])
    hooks.run_pre_hooks()
    hooks.run_post_hooks()

    assert events == ORDINARY_EVENTS * 2
    assert behavior_change_events(fired_events) == []


def test_empty_rendered_hook_is_not_executed(fired_events):
    hooks, events = make_hooks_module(behavior(), pre_hooks=[{"sql": "  ", "transaction": True}])
    hooks.run_pre_hooks()

    assert events == [("render", "  ")]
