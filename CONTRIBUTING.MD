# Contributing Guide

We happily welcome contributions to the `dbt-databricks` package. We use [GitHub Issues](https://github.com/databricks/dbt-databricks/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/dbt-databricks/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication

Before starting work on a major feature, please reach out to us via GitHub, Slack, email, etc. We will make sure no one else is already working on it and ask you to open a GitHub issue. A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.

We will use the GitHub issue to discuss the feature and come to agreement. This is to prevent your time being wasted, as well as ours. The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.

If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location. Small patches and bug fixes don't need prior communication.

## Pull Request Requirements

1. Fork our repository to make changes
1. [Use our code style](#code-style)
1. [Run the unit tests](#unit-tests) (and the [integration tests](#functional--integration-tests) if you [can](#please-test-what-you-can))
1. [Sign your commits](#sign-your-work)
1. [Open a pull request](#pull-request-review-process)
   - Answer the PR template questions as best as you can
   - _Recommended: [Allow edits from Maintainers]_

## Pull request review process

dbt-databricks uses a **two-step review process** to merge PRs to `main`. We first squash the patch onto a staging branch so that we can securely run our full matrix of integration tests against a real Databricks workspace. Then we merge the staging branch to `main`.

> **Note:** When you create a pull request we recommend that you _[Allow Edits from Maintainers]_. This smooths our two-step process and also lets your reviewer easily commit minor fixes or changes.

A dbt-databricks maintainer will review your PR and may suggest changes for style and clarity, or they may request that you add unit or integration tests.

Once your patch is approved, a maintainer will create a staging branch and either you or the maintainer (if you allowed edits from maintainers) will change the base branch of your PR to the staging branch. Then a maintainer will squash and merge the PR into the staging branch.

dbt-databricks uses staging branches to run our full matrix of functional and integration tests via Github Actions. This extra step is required for security because GH Action workflows that run on pull requests from forks can't access our testing Databricks workspace.

If the functional or integration tests fail as a result of your change, a maintainer will work with you to fix it _on your fork_ and then repeat this step.

Once all tests pass a maintainer will rebase and merge your change to `main` so that your authorship is maintained in our commit history and GitHub statistics.

---

## Developing this repository

See [docs/dbt-databricks-dev.md](docs/dbt-databricks-dev.md).

## Code Style

We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with one exception: lines can be up to 100 characters in length, not 79. You can run [`tox` linter command](#linting) to automatically format the source code before committing your changes.

### Linting

This project uses [Black](https://pypi.org/project/black/), [flake8](https://flake8.pycqa.org/en/latest/), and [mypy](https://www.mypy-lang.org/) for linting and static type checks. Run all three with the `linter` command and commit before opening your pull request.

```
tox -e linter
```

To simplify reviews you can commit any format changes in a separate commit.

Alternatively, [install pre-commit hooks](https://pre-commit.com/#3-install-the-git-hook-scripts) and the linting will be run automatically prior to accepting your commit.

## Sign your work

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch. The rules are pretty simple: if you can certify the below (from developercertificate.org):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Then you just add a line to every git commit message:

```
Signed-off-by: Joe Smith <joe.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`.

## Unit tests

Unit tests do not require a Databricks account. Please confirm that your pull request passes our unit test suite before opening a pull request.

```bash
tox -e unit
```

## Functional Tests

Functional tests require a Databricks account with access to a workspace containing three specific compute resources as detailed below.
The `tox` commands to run each set of these tests appear below:

| Compute Type        | Unity Catalog | Command                                         |
| ------------------- | ------------- | ----------------------------------------------- |
| SQL Warehouse       | Yes           | `tox -e integration-databricks-uc-sql-endpoint` |
| All Purpose Cluster | Yes           | `tox -e integration-databricks-uc-cluster`      |
| All Purpose         | No            | `tox -e integration-databricks-cluster`         |

These tests are configured with environment variables that `tox` reads from a file called [test.env](/test.env.example) which you can copy from the example:

```sh
cp test.env.example test.env
```

Update `test.env` with the relevant HTTP paths and tokens.

### Please test what you can

We understand that not every contributor will have all three types of compute resources in their Databricks workspace.
For this reason, once a change has been reviewed and merged into a staging branch, we will run the full matrix of tests against our testing workspace at our expense (see the [pull request review process](#pull-request-review-process) for more detail).

That said, we ask that you include integration tests where relevant and that you indicate in your pull request description the environment type(s) you tested the change against.

[Allow Edits from Maintainers]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork

---

## dbt-databricks-enhanced — Fork Release Process

This fork adds team-specific features and fixes on top of upstream releases. The sections below apply only to maintainers of this fork.

### Branch Model

This repository mirrors the upstream `dbt-databricks` branch model exactly:

| Branch          | Purpose                                                                  |
|-----------------|--------------------------------------------------------------------------|
| `main`          | Active development. Next unreleased enhancement lands here first.        |
| `X.Y.latest`    | Stable release line for `X.Y.Z.N` enhanced releases (e.g. `1.11.latest`). |

Rules:
- New feature work targets `main`.
- Release-line branches (`X.Y.latest`) only receive: upstream syncs for their version line, and cherry-picked fixes from `main` or from another release line.
- Tags are conceptually owned by release-line branches — tag commits must be reachable from the corresponding `X.Y.latest` branch.
- A new `X.Y.latest` branch is created when we pick up a new upstream minor version (e.g. `1.12.latest` is created when we sync upstream `1.12.0`).

### Direction of Flow

Because this fork both **develops its own enhancements** and **tracks upstream releases**, changes flow in different directions depending on their origin. Keep `main` ahead of (or equal to) every `X.Y.latest` branch so that new release lines can always branch from `main` with confidence.

| Change type             | Where to develop   | Then propagate to                                      |
|-------------------------|--------------------|--------------------------------------------------------|
| Enhancement feature     | `main`             | Cherry-pick forward to each `X.Y.latest` that needs it |
| Urgent hotfix           | `X.Y.latest`       | Merge / cherry-pick **backward to `main`** after release so `main` doesn't lose the fix |
| Upstream sync           | `X.Y.latest` only  | Don't propagate back to `main` — `main` picks up upstream changes when its own baseline is bumped |
| Shared docs / CI        | `main`             | Merge forward to each `X.Y.latest` so release-line contributors see them too |

**The backward-hotfix rule is the one people forget.** If you ship `v1.11.7.2` with a hotfix only on `1.11.latest`, immediately open a follow-up that brings the same change to `main`, otherwise the next time someone branches from `main` (e.g. to create `1.12.latest`) the fix vanishes.

**Starting point for a new `X.Y.latest` branch** (e.g. creating `1.12.latest` when upstream ships 1.12.0): always branch from `main`, then merge `upstream/1.12.latest` on top. Branching from an older `X.Y.latest` risks missing hotfixes that have already been merged back to `main`.

### Versioning

Upstream controls `major.minor.patch` (e.g. `1.11.6`). Our enhancements append a fourth segment:

```
1.11.6.1  ← first enhanced release on top of upstream 1.11.6
1.11.6.2  ← second enhanced release on 1.11.6 baseline
1.11.7.1  ← first enhanced release on top of upstream 1.11.7
```

When an `X.Y.latest` branch syncs a new upstream patch release, the fourth segment resets to `.1`.

Parallel release lines run side-by-side: `1.11.latest` may be at `1.11.7.2` while a future `1.12.latest` is at `1.12.0.1`. This lets us hotfix an older line after we've already adopted a newer one.

### Post-Merge Release Checklist

After a PR is merged into an `X.Y.latest` branch (or cherry-picked into one):

- [ ] Confirm `dbt/adapters/databricks/__version__.py` on the release branch has the correct `X.Y.Z.N` version
- [ ] Confirm `CHANGELOG.md` has a `## dbt-databricks-enhanced X.Y.Z.N (YYYY-MM-DD)` section describing **only our changes** (not upstream changes)
- [ ] Tag the release commit on the `X.Y.latest` branch:
  ```bash
  git checkout X.Y.latest && git pull
  git tag -a vX.Y.Z.N -m "X.Y.Z.N — <one-line summary>"
  git push origin vX.Y.Z.N
  ```
- [ ] Create a GitHub Release from the tag, using the changelog section as release notes
- [ ] Confirm the `Release Wheel` workflow (`.github/workflows/release-wheel.yml`) ran and attached `*.whl` + `*.tar.gz` to the release. If it didn't trigger, re-run it manually via `workflow_dispatch` with the tag name.

### Installing a Released Wheel

Consumers install a specific enhanced release directly from the GitHub Release artifact:

```
dbt-databricks @ https://github.com/viadkt/dbt-databricks-enhanced/releases/download/vX.Y.Z.N/dbt_databricks-X.Y.Z.N-py3-none-any.whl
```

The `Release Wheel` GitHub Actions workflow builds and uploads the artifact automatically when a `vX.Y.Z.N` tag is pushed.

### Changelog format

`CHANGELOG.md` contains entries for all release lines, ordered newest-first. Each entry describes only changes introduced in this repository; upstream changes are tracked by the upstream version in the header.

```markdown
## dbt-databricks-enhanced 1.11.7.1 (YYYY-MM-DD)

### Features
- ...

### Fixes
- ...

## dbt-databricks-enhanced 1.11.6.2 (YYYY-MM-DD)

### Fixes
- ...
```

### Upstream Sync

When upstream ships a new patch release (e.g. `1.11.8`):
1. Create a sync branch from the matching `X.Y.latest`: `git checkout -b sync/upstream-v1.11.8 1.11.latest`
2. Merge `upstream/1.11.latest` into it, resolve conflicts
3. Bump `__version__.py` to `1.11.8.1`
4. Update `.github/last-upstream-sync-tag` to `v1.11.8`
5. Add a changelog entry noting the upstream sync
6. Open a PR into `1.11.latest`, not `main`

When upstream ships a new minor version (e.g. `1.12.0`):
1. Create `1.12.latest` from either the newest `X.Y.latest` or `main`, depending on which contains the patches we want carried over — decide case-by-case
2. Merge `upstream/1.12.latest` into it, resolve conflicts
3. Version becomes `1.12.0.1`

### Maintaining Older Release Lines

By default, this fork follows upstream and keeps one branch per upstream minor version — for example, `1.11.latest` moves forward through `1.11.6 → 1.11.7 → 1.11.8` as we sync. A single line covers all patches within a minor version.

However, because our consumer(s) may not always be able to upgrade in lockstep, we may need to ship a fix for an older upstream patch release **after** we've already advanced `1.11.latest` past it. In that case:

1. **Tags are the anchor.** As long as a release tag exists (e.g. `v1.11.6.1`), we can always branch back from it. Tags are the permanent record of what shipped; branches are just where active work happens.
2. **Spin up a patch-line branch on demand.** When we need to fix `1.11.6.x` while `1.11.latest` is already at `1.11.7.x`, create `1.11.6.latest` from the last tag on that line:
   ```bash
   git checkout -b 1.11.6.latest v1.11.6.1
   ```
3. **Apply the fix, bump version, tag.** Fourth segment increments within the patch line (e.g. `v1.11.6.2`).
4. **Optionally delete the branch after release.** The tag preserves the release; the branch can be recreated next time we need it.

Only spin up a patch-line branch when we have an active reason to maintain that version. Otherwise, prefer moving `1.11.latest` forward and asking consumers to upgrade.

### Backporting Fixes Across Release Lines

When a fix is needed on multiple release lines:
1. Develop and merge to `main` (if the fix is also relevant to future work).
2. Cherry-pick onto each affected `X.Y.latest` (or `X.Y.Z.latest`) branch: `git cherry-pick <commit>`
3. Each line gets its own version bump, changelog entry, and tag.
