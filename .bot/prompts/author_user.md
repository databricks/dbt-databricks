## Bug to fix — issue #{{issue_number}}

**{{issue_title}}**
{{issue_url}}

### Issue description
{{issue_body}}

---

Reproduce this bug with a failing **unit** test (under `tests/unit/`), then fix
the code in `dbt/` so it passes, per the BUG-FIX FLOW and author-system rules.
The issue body above is the reporter's account — verify it against the actual
code before deciding what to change. If the behaviour is already correct, report
`no_change_needed` and say where the existing tests cover it. If the bug can only
be reproduced against a live warehouse (no unit-level repro), report `blocked`
and explain.
