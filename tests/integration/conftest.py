def pytest_configure(config):
    config.addinivalue_line(
        "markers", "profile_databricks_cluster"
    )
    config.addinivalue_line(
        "markers", "profile_databricks_sql_endpoint"
    )
