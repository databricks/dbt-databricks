# Contributing Guide

We happily welcome contributions to the `dbt-databricks` package. We use [GitHub Issues](https://github.com/databricks/dbt-databricks/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/dbt-databricks/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication
Before starting work on a major feature, please reach out to us via GitHub, Slack, email, etc. We will make sure no one else is already working on it and ask you to open a GitHub issue.
A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.
We will use the GitHub issue to discuss the feature and come to agreement.
This is to prevent your time being wasted, as well as ours.
The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.
If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.
Specifically, if the goal is to add a new extension, please read the extension policy.
Small patches and bug fixes don't need prior communication.

## Coding Style
We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with one exception: lines can be up to 100 characters in length, not 79.

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

## Set up your environment

We recommend using a virtual environment for development. To install the project dependencies run:

```python
pip install -r requirements.txt -r dev-requirements.txt
``` 

### Tests

This project uses [tox](https://tox.wiki/en/latest/) with `pytest` to run unit, functional, and integration tests. Run unit tests with `tox -e unit`.

#### Unit tests

Unit tests do not require a Databricks account.

```bash
tox -e unit
```
#### Only a specific test file

```bash
poetry run python -m pytest tests/unit/tests.py
```

#### Only a specific method

```bash
poetry run python -m pytest tests/unit/tests.py::ClientTestSuite::test_closing_connection_closes_commands
```

#### e2e Tests

End-to-end tests require a Databricks account. Before you can run them, you must set connection details for a Databricks SQL endpoint in your environment:

```bash
export host=""
export http_path=""
export access_token=""
```

There are several e2e test suites available:
- `PySQLCoreTestSuite`
- `PySQLLargeQueriesSuite`
- `PySQLStagingIngestionTestSuite`
- `PySQLRetryTestSuite.HTTP503Suite` **[not documented]**
- `PySQLRetryTestSuite.HTTP429Suite` **[not documented]**
- `PySQLUnityCatalogTestSuite` **[not documented]**

To execute the core test suite:

```bash
poetry run python -m pytest tests/e2e/driver_tests.py::PySQLCoreTestSuite
```

The `PySQLCoreTestSuite` namespace contains tests for all of the connector's basic features and behaviours. This is the default namespace where tests should be written unless they require specially configured clusters or take an especially long-time to execute by design.

The `PySQLLargeQueriesSuite` namespace contains long-running query tests and is kept separate. In general, if the `PySQLCoreTestSuite` passes then these tests will as well.

The `PySQLStagingIngestionTestSuite` namespace requires a cluster running DBR version > 12.x which supports staging ingestion commands.

The suites marked `[not documented]` require additional configuration which will be documented at a later time.

### Linting

This project uses [Black](https://pypi.org/project/black/), [flake8](https://flake8.pycqa.org/en/latest/), and [mypy](https://www.mypy-lang.org/) for linting and static type checks. Run all three with the `linter` command:

```
tox -e linter
```

To simplify reviews you can format your changes in a separate commit.