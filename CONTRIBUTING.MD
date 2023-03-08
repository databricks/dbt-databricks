# Contributing Guide

We happily welcome contributions to the `dbt-databricks` package. We use [GitHub Issues](https://github.com/databricks/dbt-databricks/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/dbt-databricks/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication
Before starting work on a major feature, please reach out to us via GitHub, Slack, email, etc. We will make sure no one else is already working on it and ask you to open a GitHub issue. A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.

We will use the GitHub issue to discuss the feature and come to agreement. This is to prevent your time being wasted, as well as ours. The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.

If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location. Small patches and bug fixes don't need prior communication.

# Pull Request Requirements

1. Fork our repository to make changes
1. [Use our code style](#code-style). This is checked automatically for all pull requests.
1. [Run the unit tests](#unit-tests) (and [the integration tests](#functional--integration-tests) if you can). We run unit tests automatically
1. [Sign your commits](#sign-your-work)
1. [Open a pull request](#pull-request-review-process)
    - Answer the PR template questions as best as you can
    - _Recommended: [Allow edits from Maintainers]_


## Pull request review process

dbt-databricks uses a **two-step review process** to merge PRs to `main`. We first squash patches to a staging branch that mirrors `main` so that we can run our full matrix of integration tests. Once these pass, the staging branch is merged to `main`.

> **Note:** When you create a pull request we recommend that you _[Allow Edits from Maintainers]_. This smooths our two-step process and also lets your reviewer easily commit minor fixes or changes.

A dbt-databricks maintainer will review your PR and may suggest changes for style and clarity, or they may request that you add unit or integration tests. 

Once your patch is approved, a maintainer will create a staging branch from the latest `main`. Either you or the maintainer (if they have edit permission) will change the base branch of your PR to the staging branch. Then a maintainer will squash and merge the PR into the staging branch.

dbt-databricks uses staging branches to run our full matrix of functional and integration tests via Github Actions. This extra step is required for security because GH Action workflows that run on pull requests from forks can't access our testing Databricks workspace. 

If any of the functional or integration tests fail as a result of your change, a maintainer will work with you to resolve them _on your fork_ and then repeat this step.

When all tests and checks have passed, a maintainer will rebase and merge your change to `main` so that your authorship is maintained in our commit history and GitHub statistics.

---

## Developing this repository

See [docs/local-dev.md](docs/local-dev.md).

## Code Style

We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with one exception: lines can be up to 100 characters in length, not 79. You can run [`tox` linter command](#linting) to automatically format the source code before committing your changes.

### Linting

This project uses [Black](https://pypi.org/project/black/), [flake8](https://flake8.pycqa.org/en/latest/), and [mypy](https://www.mypy-lang.org/) for linting and static type checks. Run all three with the `linter` command and commit before opening your pull request.

```
tox -e linter
```

To simplify reviews you can commit any format changes in a separate commit.

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


### Unit tests

Unit tests do not require a Databricks account. Please confirm that your pull request passes our unit test suite before opening a pull request.

```bash
tox -e unit
```

### Functional & Integration Tests

Functional and integration tests require a Databricks account with access to a workspace containing four compute resources. These four comprise a matrix of multi-purpose cluster vs SQL warehouse with and without Unity Catalog enabled. The `tox` commands to run each set of these tests appear below:

|Compute Type |Unity Catalog |Command|
|-|-|-|
|SQL warehouse| Yes | `tox -e integration-databricks-uc-sql-endpoint`  |
|SQL warehouse| No | `tox -e integration-databricks-sql-endpoint` |
|Multi-purpose| Yes |  `tox -e integration-databricks-uc-cluster` |
|Multi-Purpose| No | `tox -e integration-databricks-cluster` |

These tests are configured with environment variables that `tox` reads from a file called [test.env](/test.env.example) which you can copy from the example:

```sh
cp test.env.example test.env
```

Update `test.env` with the relevant HTTP paths and tokens. 


#### Please test what you can
We understand that not every contributor will have all four types of compute resources in their Databricks workspace. For this reason, once a change has been reviewed and merged into a staging branch, we will run the full matrix of tests against our testing workspace at our expense (see the [pull request review process](#opening-a-pull-request) for more detail).

That said, we ask that you include integration tests where relevant and that you indicate in your pull request description the environment type(s) you tested the change against.



[Allow Edits from Maintainers]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork