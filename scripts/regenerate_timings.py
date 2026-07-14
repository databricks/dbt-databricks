#!/usr/bin/env python3
"""Regenerate `.github/test_timings.json` from recent green integration runs.

`shard_assign.py`'s `lpt_historical_time` algorithm balances the integration
matrix on per-file wall time read from `.github/test_timings.json`. When that
file goes stale (new tests land without it being refreshed), the balancer
mean-fills the unknown files and the shards drift out of balance, so the matrix
wall-clock — set by its slowest shard — grows.

This is the automatable, all-profiles-at-once front end to
`refresh_timings.py` (which aggregates one profile from one run's junit XMLs).
It discovers recent green `integration.yml` runs, downloads every profile's
per-shard junit artifacts, and writes the **median** per-file wall time across
runs — median rather than mean so a single slow-warehouse run doesn't skew a
file's weight.

Usage:

  # Auto-pick the last 3 distinct-SHA green integration runs and rewrite the file
  python scripts/regenerate_timings.py

  # Pin exact runs (reproducible; what a future auto-PR job would pass)
  python scripts/regenerate_timings.py --run-ids 28968407123 28901569145 28826166614

Requires the `gh` CLI authenticated for the target repo. Reuses the junit
parsing in `refresh_timings.py`; the output format is byte-identical to it
(profile-keyed dict of file_path -> wall seconds, rounded to ms, sorted).
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
from pathlib import Path

from refresh_timings import aggregate_per_file

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


def total_wall_by_profile(doc: dict[str, dict[str, float]]) -> dict[str, float]:
    """Total per-profile wall seconds (sum of every file's time).

    This is the drift signal the weekly refresh workflow gates on. Crucially it
    is **independent of shard count**: it does not need to know how many shards
    a profile runs on, so it stays correct even if integration.yml changes a
    profile's shard count. (Projecting per-shard balance instead would force
    this code to duplicate integration.yml's shard counts and silently go wrong
    when they change.)
    """
    return {profile: round(sum(by_file.values()), 3) for profile, by_file in doc.items()}


def drift_by_profile(
    old_doc: dict[str, dict[str, float]],
    new_doc: dict[str, dict[str, float]],
) -> dict[str, float]:
    """Per-profile percent change in total wall time from old to new timings.

    Measured as an absolute percentage (direction-agnostic — a human reviews
    the numbers either way). A profile present in the new file but absent from
    the old one (or with zero old total) reports `inf`, so the caller's
    pathological guard flags it for manual review rather than trusting an
    unbounded change.
    """
    old_totals = total_wall_by_profile(old_doc)
    new_totals = total_wall_by_profile(new_doc)
    drift: dict[str, float] = {}
    for profile in sorted(set(old_totals) | set(new_totals)):
        old = old_totals.get(profile, 0.0)
        new = new_totals.get(profile, 0.0)
        if old == 0.0:
            drift[profile] = 0.0 if new == 0.0 else float("inf")
        else:
            drift[profile] = abs(new - old) / old * 100.0
    return drift


def max_drift_pct(drift: dict[str, float]) -> float:
    """Worst per-profile drift — the single number the PR gate compares."""
    return max(drift.values()) if drift else 0.0


def format_drift_summary(
    old_doc: dict[str, dict[str, float]],
    new_doc: dict[str, dict[str, float]],
    drift: dict[str, float],
) -> str:
    """Markdown table of per-profile old/new total wall + drift, for a PR body."""
    old_totals = total_wall_by_profile(old_doc)
    new_totals = total_wall_by_profile(new_doc)
    lines = [
        "| profile | old total | new total | drift |",
        "| --- | --- | --- | --- |",
    ]
    for profile in sorted(drift):
        old_m = old_totals.get(profile, 0.0) / 60
        new_m = new_totals.get(profile, 0.0) / 60
        pct = drift[profile]
        pct_str = "n/a (new profile)" if pct == float("inf") else f"{pct:.1f}%"
        lines.append(f"| `{profile}` | {old_m:.1f}m | {new_m:.1f}m | {pct_str} |")
    worst = max_drift_pct(drift)
    worst_str = "n/a (new profile)" if worst == float("inf") else f"{worst:.1f}%"
    lines.append("")
    lines.append(f"**Max per-profile total-wall drift: {worst_str}**")
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
            "Path to the pre-existing test_timings.json to compare against. When "
            "given, report per-profile total-wall drift (old vs newly generated)."
        ),
    )
    p.add_argument(
        "--drift-out",
        default=None,
        help=(
            "Write a JSON drift report {max_drift_pct, per_profile, summary} to "
            "this path (requires --old). Consumed by the refresh workflow's PR gate."
        ),
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

        merged = median_across_runs(per_run)
    finally:
        if not args.keep_downloads:
            shutil.rmtree(tmp_root, ignore_errors=True)
        else:
            print(f"  kept downloads in {tmp_root}", flush=True)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Round to ms for compactness; per-test variance is far above 1 ms anyway.
    out = {
        profile: {fp: round(t, 3) for fp, t in sorted(by_file.items())}
        for profile, by_file in sorted(merged.items())
    }
    output_path.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")

    print(f"\nWrote {output_path} (median of {len(per_run)} run(s)):", flush=True)
    for profile, by_file in sorted(merged.items()):
        vals = by_file.values()
        max_file = max(by_file, key=lambda f: by_file[f]) if by_file else "-"
        print(
            f"  {profile}: {len(by_file)} files, total {sum(vals) / 60:.1f}m, "
            f"max-file {max(vals) / 60:.1f}m ({max_file})",
            flush=True,
        )

    # Drift report vs the pre-existing timings. Per-profile total-wall %, which
    # is independent of shard count — the decision lives here (tested, reused by
    # manual regens) rather than in workflow YAML that would duplicate
    # integration.yml's shard counts.
    if args.old:
        old_doc = json.loads(Path(args.old).read_text())
        drift = drift_by_profile(old_doc, out)
        worst = max_drift_pct(drift)
        summary = format_drift_summary(old_doc, out, drift)
        print(f"\n{summary}", flush=True)
        if args.drift_out:
            # json.dumps emits the invalid-JSON token `Infinity` for float inf;
            # serialize inf as the string "inf" instead (float("inf") round-trips
            # it, and the workflow's math.isinf catches it as pathological).
            def _json_safe(v: float) -> float | str:
                return "inf" if v == float("inf") else v

            report = {
                "max_drift_pct": _json_safe(worst),
                "runs_used": len(per_run),
                "per_profile": {p: _json_safe(d) for p, d in drift.items()},
                "summary": summary,
            }
            Path(args.drift_out).write_text(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
