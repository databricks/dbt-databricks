def pytest_configure(config):
    config.addinivalue_line(
        "markers", "profile_databricks_sql_connector"
    )
