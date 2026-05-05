#!/usr/bin/env python3
"""Verify that a sharded pytest run covered every assigned test exactly once.

Inputs:
- A directory of junit XML files written by `pytest --junitxml=...`,
  one per shard (e.g. `junit-shard-0.xml`, `junit-shard-1.xml`).
- The manifest JSON written by `shard_assign.py` and the per-shard nodeid
  list files it wrote.

Invariants asserted:
  I1  Coverage:   union of nodeids actually executed == set of all assigned
                  nodeids from manifest's per-shard txt files.
  I2  Uniqueness: every nodeid appears in exactly one shard's junit (no
                  duplicate execution).
  I3  Containment: each shard's executed nodeids ⊆ its assigned nodeids
                   (no test ran on the wrong shard).
  I4  Counts:    sum of per-shard junit testcase counts == manifest's
                 total_tests.

Exits 0 if all four pass; 1 otherwise. Always prints a summary table.

Note: junit's `<testcase>` is emitted whether the test passed, failed, or
was skipped (skip emits a `<skipped/>` child). So coverage is independent
of pass/fail status.
"""
from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--manifest", required=True, help="Path to *-manifest.json from shard_assign")
    p.add_argument(
        "--shard-dir",
        required=True,
        help="Directory containing the *-shard-N.txt assignment files "
        "(same dir as manifest by convention)",
    )
    p.add_argument(
        "--junit-dir",
        required=True,
        help="Directory containing junit XML files (one per shard, named "
        "junit-shard-N.xml)",
    )
    return p.parse_args()


def junit_to_nodeid_candidates(
    file_attr: str, classname: str, name: str
) -> list[str]:
    """Return candidate pytest nodeids from junit `<testcase>` attributes.

    pytest junit XML omits `file=` in some versions; classname encodes the
    module path joined with class chain via dots. We can't tell at the
    classname level where "module" ends and "class" begins (e.g. `a.b.c.D`
    could be `a/b/c.py::D` or `a/b.py::c::D`). So we generate every
    plausible split and let the caller match against the assigned set.

    pytest nodeid format:
      "tests/functional/x/test_y.py::TestZ::test_method"   (with class)
      "tests/functional/x/test_y.py::test_method"          (no class)
      "tests/functional/x/test_y.py::TestZ::TestNested::test_m"  (nested)
    """
    candidates: list[str] = []

    # If file attribute is present, that's authoritative
    if file_attr:
        module_path = file_attr.replace("/", ".")
        if module_path.endswith(".py"):
            module_path = module_path[:-3]
        if classname == module_path:
            candidates.append(f"{file_attr}::{name}")
        elif classname.startswith(module_path + "."):
            class_chain = classname[len(module_path) + 1 :]
            candidates.append(f"{file_attr}::{class_chain.replace('.', '::')}::{name}")
        return candidates

    # Fallback: derive from classname alone. Try every possible module/class split.
    parts = classname.split(".")
    for k in range(len(parts), 0, -1):
        file_guess = "/".join(parts[:k]) + ".py"
        class_parts = parts[k:]
        if not class_parts:
            candidates.append(f"{file_guess}::{name}")
        else:
            candidates.append(f"{file_guess}::{'::'.join(class_parts)}::{name}")
    return candidates


def parse_junit(
    path: Path, assigned_universe: set[str]
) -> tuple[list[str], int, int, list[tuple[str, str]]]:
    """Return (executed_nodeids, total_count, skipped_count, unresolved).

    `assigned_universe` is the union of all assigned nodeids; used to pick the
    right split when junit's classname is ambiguous. `unresolved` lists any
    (classname, name) pairs we couldn't map to an assigned nodeid — these are
    real bugs that I1 will catch as "extra" tests.
    """
    tree = ET.parse(path)
    root = tree.getroot()
    suites = root.findall(".//testsuite") or [root]
    nodeids: list[str] = []
    total = 0
    skipped = 0
    unresolved: list[tuple[str, str]] = []
    for suite in suites:
        for tc in suite.findall("testcase"):
            file_attr = tc.get("file", "")
            classname = tc.get("classname", "")
            name = tc.get("name", "")
            cands = junit_to_nodeid_candidates(file_attr, classname, name)
            picked = next((c for c in cands if c in assigned_universe), None)
            if picked is None:
                # No candidate matches anything assigned: still record the
                # most-specific candidate so I1 will flag it as "extra".
                picked = cands[0] if cands else f"{classname}::{name}"
                unresolved.append((classname, name))
            nodeids.append(picked)
            total += 1
            if tc.find("skipped") is not None:
                skipped += 1
    return nodeids, total, skipped, unresolved


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest)
    shard_dir = Path(args.shard_dir)
    junit_dir = Path(args.junit_dir)

    manifest = json.loads(manifest_path.read_text())
    profile = manifest["profile"]
    n_shards = manifest["num_shards"]
    expected_total = manifest["total_tests"]

    # Load assigned nodeids per shard
    assigned_per_shard: list[set[str]] = []
    for i in range(n_shards):
        f = shard_dir / f"{profile}-shard-{i}.txt"
        if not f.exists():
            print(f"ERROR: missing shard file: {f}", file=sys.stderr)
            return 1
        nodeids = {ln.strip() for ln in f.read_text().splitlines() if ln.strip()}
        assigned_per_shard.append(nodeids)
    assigned_all = set().union(*assigned_per_shard)

    # Load executed nodeids per shard from junit
    executed_per_shard: list[list[str]] = []
    skipped_per_shard: list[int] = []
    unresolved_per_shard: list[list[tuple[str, str]]] = []
    for i in range(n_shards):
        jp = junit_dir / f"junit-shard-{i}.xml"
        if not jp.exists():
            print(f"ERROR: missing junit file: {jp}", file=sys.stderr)
            return 1
        nodeids, total, skipped, unresolved = parse_junit(jp, assigned_all)
        executed_per_shard.append(nodeids)
        skipped_per_shard.append(skipped)
        unresolved_per_shard.append(unresolved)

    failures: list[str] = []

    # I4 — total count
    total_executed = sum(len(s) for s in executed_per_shard)
    if total_executed != expected_total:
        failures.append(
            f"I4: total executed = {total_executed}, "
            f"manifest total_tests = {expected_total}"
        )

    # I2 — uniqueness across shards
    counts: Counter[str] = Counter()
    for s in executed_per_shard:
        counts.update(s)
    duplicates = {k: v for k, v in counts.items() if v > 1}
    if duplicates:
        failures.append(
            f"I2: {len(duplicates)} nodeids ran on more than one shard "
            f"(first 5: {list(duplicates.items())[:5]})"
        )

    # I1 — coverage
    executed_all = set(counts.keys())
    missing = assigned_all - executed_all
    extra = executed_all - assigned_all
    if missing:
        failures.append(
            f"I1: {len(missing)} assigned nodeids never executed "
            f"(first 5: {sorted(missing)[:5]})"
        )
    if extra:
        failures.append(
            f"I1: {len(extra)} executed nodeids were NOT assigned "
            f"(first 5: {sorted(extra)[:5]})"
        )

    # I3 — containment per shard
    for i, executed in enumerate(executed_per_shard):
        wrong_shard = set(executed) - assigned_per_shard[i]
        if wrong_shard:
            failures.append(
                f"I3: shard {i} ran {len(wrong_shard)} nodeids that were "
                f"assigned to other shards (first 5: {sorted(wrong_shard)[:5]})"
            )

    # Print summary
    print(f"# Shard verification — profile={profile} num_shards={n_shards}")
    print(f"  manifest total_tests = {expected_total}")
    print(f"  shard | assigned | executed | skipped")
    for i in range(n_shards):
        print(
            f"  {i:>5} | {len(assigned_per_shard[i]):>8} "
            f"| {len(executed_per_shard[i]):>8} | {skipped_per_shard[i]:>7}"
        )
    print(f"  total | {len(assigned_all):>8} | {total_executed:>8} | "
          f"{sum(skipped_per_shard):>7}")

    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll invariants passed (I1, I2, I3, I4).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
