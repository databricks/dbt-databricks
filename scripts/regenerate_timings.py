#!/usr/bin/env python3
"""Regenerate `.github/test_timings.json` from recent green integration runs.

`shard_assign.py`'s `lpt_historical_time` algorithm weights each test file by
its historical wall time (read from this JSON). When the file goes stale the
balancer mean-fills unknown files, the shards drift out of balance, and the
matrix wall-clock (set by its slowest shard) grows.

This discovers recent green `integration.yml` runs, downloads every profile's
per-shard junit artifacts, and takes the **median** per-file wall time across
runs (median so one slow-warehouse run can't skew a file's weight).

With `--old`, it merges per file into the existing timings instead of rewriting
wholesale: a new file is added, an existing file's timing is rewritten only when
it moved by more than `UPDATE_THRESHOLD_PCT` (keeps run-to-run noise out), and a
file that moved by more than `MANUAL_REVIEW_PCT` is flagged for a human. The
decision is entirely per-file, so it never needs to know shard counts.

Usage:

  # Full rewrite from the last 3 distinct-SHA green runs
  python scripts/regenerate_timings.py

  # Per-file merge vs the current file + change report (what the weekly job runs)
  python scripts/regenerate_timings.py \\
    --old .github/test_timings.json --drift-out /tmp/drift.json

Requires the `gh` CLI authenticated for the target repo.
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

# Per-file merge thresholds — the drift decision the weekly refresh workflow
# gates its PR on. Kept here (the single place) because they are tested and
# reused by manual regens; the workflow YAML stays declarative.
UPDATE_THRESHOLD_PCT = 10.0  # rewrite an existing file's timing only past this move
MANUAL_REVIEW_PCT = 60.0  # a move this large is implausible -> flag for a human

# Artifact-name prefix -> test_timings.json profile key. The artifact names are
# `<prefix>-test-logs-<pr-or-dispatch>-shard-<n>` (see integration.yml). Order
# matters: match the more specific prefixes before the bare "cluster".
PROFILE_PREFIXES: list[tuple[str, str]] = [
    ("uc-cluster", "databricks_uc_cluster"),
    ("sql-endpoint", "databricks_uc_sql_endpoint"),
    ("cluster", "databricks_cluster"),
]

INTEGRATION_WORKFLOW = "integration.yml"
ARTIFACT_GLOB = "*-test-logs-*-shard-*"


def classname_to_file(classname: str) -> str:
    """Convert a pytest junit `classname` (dotted module path + class chain)
    into a file path. The file is the segment ending in the last `test_*`
    component; everything after that is class chain.

    Examples:
      tests.functional.adapter.grants.test_grants.TestModelGrants
        → tests/functional/adapter/grants/test_grants.py
      tests.unit.test_x  (module-level test, no class)
        → tests/unit/test_x.py
    """
    parts = classname.split(".")
    # Walk from the right; the LAST test_* segment marks the file boundary.
    last_test_idx = max(
        (i for i, p in enumerate(parts) if p.startswith("test_")),
        default=-1,
    )
    if last_test_idx == -1:
        return "/".join(parts) + ".py"
    return "/".join(parts[: last_test_idx + 1]) + ".py"


def aggregate_per_file(junit_paths: list[Path]) -> dict[str, float]:
    """Sum `time` attributes across all testcases per file."""
    by_file: dict[str, float] = defaultdict(float)
    for jp in junit_paths:
        for tc in ET.parse(jp).getroot().findall(".//testcase"):
            f = classname_to_file(tc.get("classname", ""))
            by_file[f] += float(tc.get("time", "0") or 0)
    return dict(by_file)


def _profile_for_artifact_dir(name: str) -> str | None:
    """Map a downloaded artifact directory name to a profile key, or None."""
    for prefix, profile in PROFILE_PREFIXES:
        if name.startswith(f"{prefix}-test-logs-"):
            return profile
    return None


def discover_green_run_ids(repo: str, num_runs: int) -> list[str]:
    """Return the newest `num_runs` green integration runs, one per distinct SHA.

    Deduping by head SHA means re-runs of the same commit don't crowd out the
    diversity that makes the median meaningful.
    """
    out = subprocess.run(
        [
            "gh",
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            INTEGRATION_WORKFLOW,
            "--status",
            "success",
            "--limit",
            "40",
            "--json",
            "databaseId,headSha,createdAt",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    runs = json.loads(out)
    # gh returns newest-first; keep the first (newest) run per distinct SHA.
    seen_sha: set[str] = set()
    picked: list[str] = []
    for r in runs:
        sha = r["headSha"]
        if sha in seen_sha:
            continue
        seen_sha.add(sha)
        picked.append(str(r["databaseId"]))
        if len(picked) == num_runs:
            break
    if not picked:
        sys.exit("ERROR: no green integration runs found")
    return picked


def download_run_artifacts(repo: str, run_id: str, dest: Path, attempts: int = 4) -> None:
    """Download all per-shard test-log artifacts for one run into `dest`.

    The GitHub artifact API intermittently returns transient `HTTP 401: Bad
    credentials` on this large-artifact repo, so retry with a short backoff
    before giving up. `gh run download` is safe to re-run into the same dir
    (already-downloaded artifacts are simply overwritten).
    """
    dest.mkdir(parents=True, exist_ok=True)
    cmd = [
        "gh",
        "run",
        "download",
        run_id,
        "--repo",
        repo,
        "--pattern",
        ARTIFACT_GLOB,
        "--dir",
        str(dest),
    ]
    for attempt in range(1, attempts + 1):
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return
        transient = "Bad credentials" in result.stderr or "HTTP 401" in result.stderr
        if attempt == attempts or not transient:
            sys.stderr.write(result.stderr)
            result.check_returncode()  # raises CalledProcessError
        print(
            f"  download of run {run_id} hit a transient error "
            f"(attempt {attempt}/{attempts}); retrying...",
            flush=True,
        )
        time.sleep(5 * attempt)


def per_profile_timings_for_run(run_dir: Path) -> dict[str, dict[str, float]]:
    """Aggregate one run's downloaded artifacts into {profile: {file: seconds}}.

    `gh run download` lays artifacts out as `<run_dir>/<artifact-name>/<files>`;
    each artifact dir holds a single `junit-shard-<n>.xml`.
    """
    junits_by_profile: dict[str, list[Path]] = {}
    for artifact_dir in sorted(run_dir.iterdir()):
        if not artifact_dir.is_dir():
            continue
        profile = _profile_for_artifact_dir(artifact_dir.name)
        if profile is None:
            continue
        junits = sorted(artifact_dir.glob("junit-shard-*.xml"))
        if junits:
            junits_by_profile.setdefault(profile, []).extend(junits)
    return {profile: aggregate_per_file(junits) for profile, junits in junits_by_profile.items()}


def median_across_runs(
    per_run: list[dict[str, dict[str, float]]],
) -> dict[str, dict[str, float]]:
    """Median per-file wall time per profile across runs.

    A file present in only some runs is medianed over just the runs that have
    it; a file present in none stays absent (shard_assign.py mean-fills those).
    """
    profiles = {p for run in per_run for p in run}
    merged: dict[str, dict[str, float]] = {}
    for profile in sorted(profiles):
        run_files = [run.get(profile, {}) for run in per_run]
        files = {f for rf in run_files for f in rf}
        merged[profile] = {}
        for f in files:
            samples = [rf[f] for rf in run_files if f in rf]
            merged[profile][f] = statistics.median(samples)
    return merged


def _pct_delta(old: float, new: float) -> float:
    """Absolute percent change old->new; `inf` when there's no old baseline."""
    if old == 0.0:
        return 0.0 if new == 0.0 else float("inf")
    return abs(new - old) / old * 100.0


@dataclass
class FileChange:
    """One per-file change made by the merge. `old` is None for a new file, in
    which case `delta_pct` is 0.0 (a new file is never manual-review material)."""

    profile: str
    file: str
    new: float
    old: float | None = None
    delta_pct: float = 0.0


def merge_timings(
    old_doc: dict[str, dict[str, float]],
    fresh: dict[str, dict[str, float]],
    update_threshold_pct: float = UPDATE_THRESHOLD_PCT,
) -> tuple[dict[str, dict[str, float]], list[FileChange]]:
    """Merge freshly generated timings into the existing file, per file.

    A new file is added; an existing file's timing is rewritten only when it
    moved by more than `update_threshold_pct` (keeps run-to-run noise out). A
    file with no fresh sample keeps its old value. Returns the merged doc and
    the list of `FileChange`s applied.
    """
    merged: dict[str, dict[str, float]] = {}
    changes: list[FileChange] = []
    for profile in sorted(set(old_doc) | set(fresh)):
        old_p = old_doc.get(profile, {})
        new_p = fresh.get(profile, {})
        merged_p = dict(old_p)  # keep files not re-measured this round
        for f in sorted(new_p):
            new_val = new_p[f]
            if f not in old_p:
                merged_p[f] = new_val
                changes.append(FileChange(profile=profile, file=f, new=new_val))
            else:
                delta = _pct_delta(old_p[f], new_val)
                if delta > update_threshold_pct:
                    merged_p[f] = new_val
                    changes.append(
                        FileChange(
                            profile=profile, file=f, new=new_val, old=old_p[f], delta_pct=delta
                        )
                    )
        merged[profile] = merged_p
    return merged, changes


def manual_review_changes(
    changes: list[FileChange],
    manual_review_pct: float = MANUAL_REVIEW_PCT,
) -> list[FileChange]:
    """Updated files whose move is implausibly large — a human should look."""
    return [c for c in changes if c.delta_pct > manual_review_pct]


def format_change_summary(changes: list[FileChange], manual: list[FileChange]) -> str:
    """Markdown summary of the per-file merge, for a PR body / workflow log."""
    new_files = [c for c in changes if c.old is None]
    updated = [c for c in changes if c.old is not None]
    lines = [
        f"- New files added: {len(new_files)}",
        f"- Existing files updated (moved >{UPDATE_THRESHOLD_PCT:g}%): {len(updated)}",
    ]
    rows = manual if manual else sorted(updated, key=lambda c: -c.delta_pct)[:10]
    if rows:
        heading = (
            f"**{len(manual)} file(s) moved >{MANUAL_REVIEW_PCT:g}% — needs manual review:**"
            if manual
            else "Largest updates:"
        )
        lines += [
            "",
            heading,
            "",
            "| profile | file | old | new | delta |",
            "| --- | --- | --- | --- | --- |",
        ]
        for c in rows:
            old_s = f"{c.old:.1f}s" if c.old is not None else "—"
            lines.append(
                f"| `{c.profile}` | `{c.file}` | {old_s} | {c.new:.1f}s | {c.delta_pct:.0f}% |"
            )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--output",
        default=".github/test_timings.json",
        help="Path to test_timings.json (overwritten; default: .github/test_timings.json)",
    )
    p.add_argument(
        "--run-ids",
        nargs="+",
        metavar="RUN_ID",
        help="Explicit integration.yml run IDs to aggregate (default: auto-discover green runs)",
    )
    p.add_argument(
        "--num-runs",
        type=int,
        default=3,
        help="How many distinct-SHA green runs to aggregate when auto-discovering (default: 3)",
    )
    p.add_argument(
        "--repo",
        default="databricks/dbt-databricks",
        help="owner/repo for gh (default: databricks/dbt-databricks)",
    )
    p.add_argument(
        "--keep-downloads",
        action="store_true",
        help="Keep the downloaded junit artifacts instead of deleting the temp dir",
    )
    p.add_argument(
        "--old",
        default=None,
        help=(
            "Existing test_timings.json to merge into per file (new files added, "
            "existing files updated only past the threshold). Without it, rewrite wholesale."
        ),
    )
    p.add_argument(
        "--drift-out",
        default=None,
        help="Write a JSON change report (requires --old). Consumed by the refresh workflow.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    run_ids = args.run_ids or discover_green_run_ids(args.repo, args.num_runs)
    print(f"Aggregating {len(run_ids)} run(s): {', '.join(run_ids)}", flush=True)

    tmp_root = Path(tempfile.mkdtemp(prefix="regen-timings-"))
    try:
        per_run: list[dict[str, dict[str, float]]] = []
        for run_id in run_ids:
            run_dir = tmp_root / run_id
            download_run_artifacts(args.repo, run_id, run_dir)
            timings = per_profile_timings_for_run(run_dir)
            if not timings:
                print(f"  WARNING: run {run_id} yielded no timings", file=sys.stderr)
                continue
            for profile, by_file in timings.items():
                print(
                    f"  run {run_id} {profile}: {len(by_file)} files, "
                    f"{sum(by_file.values()) / 60:.1f}m",
                    flush=True,
                )
            per_run.append(timings)

        if not per_run:
            print("ERROR: no timings parsed from any run", file=sys.stderr)
            return 1

        fresh = median_across_runs(per_run)
    finally:
        if not args.keep_downloads:
            shutil.rmtree(tmp_root, ignore_errors=True)
        else:
            print(f"  kept downloads in {tmp_root}", flush=True)

    changes: list[FileChange] | None = None
    if args.old:
        old_doc = json.loads(Path(args.old).read_text())
        final_doc, changes = merge_timings(old_doc, fresh)
    else:
        final_doc = fresh

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Round to ms for compactness; per-test variance is far above 1 ms anyway.
    out = {
        profile: {fp: round(t, 3) for fp, t in sorted(by_file.items())}
        for profile, by_file in sorted(final_doc.items())
    }
    output_path.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")

    print(f"\nWrote {output_path} (median of {len(per_run)} run(s)):", flush=True)
    for profile, by_file in sorted(final_doc.items()):
        vals = by_file.values()
        max_file = max(by_file, key=lambda f: by_file[f]) if by_file else "-"
        print(
            f"  {profile}: {len(by_file)} files, total {sum(vals) / 60:.1f}m, "
            f"max-file {max(vals) / 60:.1f}m ({max_file})",
            flush=True,
        )

    if changes is not None:
        manual = manual_review_changes(changes)
        summary = format_change_summary(changes, manual)
        print(f"\n{summary}", flush=True)
        if args.drift_out:
            report = {
                "runs_used": len(per_run),
                "new_files": sum(1 for c in changes if c.old is None),
                "updated_files": sum(1 for c in changes if c.old is not None),
                "needs_manual_review": bool(manual),
                "manual_review_files": [asdict(c) for c in manual],
                "summary": summary,
            }
            Path(args.drift_out).write_text(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
