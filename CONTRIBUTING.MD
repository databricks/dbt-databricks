# Contributing Guide

We happily welcome contributions to the `dbt-databricks` package. We use [GitHub Issues](https://github.com/databricks/dbt-databricks/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/dbt-databricks/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication
Before starting work on a major feature, please reach out to us via GitHub, Slack, email, etc. We will make sure no one else is already working on it and ask you to open a GitHub issue. A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior. We will use the GitHub issue to discuss the feature and come to agreement. This is to prevent your time being wasted, as well as ours. The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design. If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location. Small patches and bug fixes don't need prior communication.

## Opening a Pull Request

1. To contribute, please fork this repository and commit your changes to a feature branch including a [DCO Sign-off](#sign-your-work).
2. Open your pull request and answer the questions in the pull request template as best as you can.
3. A Databricks employee will work with you to review your pull request, suggest any changes, and verify that all pre-merge checks can pass (linting and unit tests). To keep this process smooth we recommend that you grant your PR reviewer commit permission to the feature branch of your fork on GitHub. This helps us unblock merging a PR for easy-to-fix nits like code formatting. 
4. We will merge approved pull requests into the `main` branch in a two-step process:
    a.  First, a repository owner will create a staging branch from the latest `main`. This staging branch will include your PR number (example `staging-100`). Your change will be squashed and merged into this staging branch first.
    b. Second, a repository owner will open a pull request to merge the staging branch into `main`. This step is needed to run our full matrix of functional and integration tests via Github Actions. For security purposes, GH Action workflows that run on pull requests from forks can't access our testing Databricks workspace. 
    c. If any of the functional or integration tests fail as a result of your change, we will work with you to resolve them _on your fork_ and then repeat steps 3 and 4.
5. When all tests and checks have passed, a Databricks employee will rebase and merge your change to `main` so that your authorship is maintained in our commit history and GitHub statistics.

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

#### Functional Tests

TBD

#### Integration Tests

TBD

### Linting

This project uses [Black](https://pypi.org/project/black/), [flake8](https://flake8.pycqa.org/en/latest/), and [mypy](https://www.mypy-lang.org/) for linting and static type checks. Run all three with the `linter` command:

```
tox -e linter
```

To simplify reviews you can format your changes in a separate commit.