import pandas as pd  # type: ignore
from pip._internal.operations import freeze


def model(dbt, spark):
    dbt.config(
        packages=["databricks-sql-connector==2.7.0"],
        index_url="https://pypi.org/simple",
    )
    pkgs = freeze.freeze()
    for pkg in pkgs:
        print(pkg)

    data = [[1, "Elia"], [2, "Teo"], [3, "Fang"]]

    pdf = pd.DataFrame(data, columns=["id", "name"])

    df = spark.createDataFrame(pdf)

    return df
