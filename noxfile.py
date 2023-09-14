from nox_poetry import session


@session(tags=["lint", "lint_check"])
def black(session):
    session.install("black")
    session.run("black", "--check", "dbt", "tests")


@session(tags=["lint"])
def black_fix(session):
    session.install("black")
    session.run("black", "dbt", "tests")


@session(tags=["lint", "lint_check"])
def flake8(session):
    session.install("flake8")
    session.run(
        "flake8", "--select=E,W,F", "--ignore=E203,W503", "--max-line-length=100", "dbt", "tests"
    )


@session(tags=["lint", "lint_check"])
def mypy(session):
    session.run_always("poetry", "install", external=True)
    session.run("mypy", "--explicit-package-bases", "dbt", "tests")


@session(python=["3.8", "3.9", "3.10", "3.11"])
def unit(session):
    session.run_always("poetry", "install", external=True)
    session.run("pytest", "--color=yes", "-v", "tests/unit")


@session(tags=["cluster", "integration"])
def cluster_integration(session):
    session.run_always("poetry", "install", external=True)
    session.run(
        "pytest",
        "-v",
        "-m",
        "profile_databricks_cluster",
        "-n4",
        "tests/integration",
    )


@session(tags=["cluster", "functional"])
def cluster_functional(session):
    session.run_always("poetry", "install", external=True)
    session.run(
        "pytest",
        "-v",
        "--profile",
        "databricks_cluster",
        "-n4",
        "tests/functional",
    )


@session(tags=["uc_cluster", "integration"])
def uc_cluster_integration(session):
    session.run_always("poetry", "install", external=True)
    session.run(
        "pytest",
        "-v",
        "-m",
        "profile_databricks_uc_cluster",
        "-n4",
        "tests/integration",
    )


@session(tags=["uc_cluster", "functional"])
def uc_cluster_functional(session):
    session.run_always("poetry", "install", external=True)
    session.run(
        "pytest",
        "-v",
        "--profile",
        "databricks_uc_cluster",
        "-n4",
        "tests/functional",
    )


@session(tags=["uc_sql_endpoint", "integration"])
def uc_sql_integration(session):
    session.run_always("poetry", "install", external=True)
    session.run(
        "pytest",
        "-v",
        "-m",
        "profile_databricks_uc_sql_endpoint",
        "-n4",
        "tests/integration",
    )


@session(tags=["uc_sql_endpoint", "functional"])
def uc_sql_functional(session):
    session.run_always("poetry", "install", external=True)
    session.run(
        "pytest",
        "-v",
        "--profile",
        "databricks_uc_sql_endpoint",
        "-n4",
        "tests/functional",
    )
