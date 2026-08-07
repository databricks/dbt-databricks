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

When you open a PR, the unit-test / lint / build checks run automatically. The full integration test matrix (against a real Databricks workspace) is **not** run automatically — it's an explicit maintainer decision, because those tests require compute resources.

A dbt-databricks maintainer will review your PR and may suggest changes for style and clarity, or they may request that you add unit or integration tests.

> **Note:** When you create a pull request we recommend that you _[Allow Edits from Maintainers]_ so a reviewer can easily commit minor fixes.

When the reviewer is ready to run the integration matrix, they'll comment `/integration-test` on the PR. This works the same way for PRs from forks — the command runs the tests in the main repo context so they can use the Databricks workspace. Only repo maintainers can issue the command; comments from other users get a reply explaining this.

The PR will receive a reply comment when the dispatch succeeds and a follow-up comment with the per-job pass/fail when the run completes.

If the integration tests fail as a result of your change, a maintainer will work with you to fix it _on your fork_ and re-run `/integration-test`.

Once all tests pass a maintainer will rebase and merge your change to `main` so that your authorship is maintained in our commit history and GitHub statistics. `main` is additionally covered by a nightly integration run that skips itself if the branch hasn't advanced since the last green run.

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
For this reason, once a change has been reviewed, a maintainer will run the full matrix of tests against our testing workspace at our expense via the `/integration-test` comment command (see the [pull request review process](#pull-request-review-process) for more detail).

That said, we ask that you include integration tests where relevant and that you indicate in your pull request description the environment type(s) you tested the change against.

[Allow Edits from Maintainers]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork
