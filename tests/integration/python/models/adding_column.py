import pandas as pd  # type: ignore


def model(dbt, spark):
    dbt.config(materialized="incremental")
    dbt.config(unique_key="name")
    dbt.config(on_schema_change="append_new_columns")
    if dbt.is_incremental:
        data = [[2, "Teo", "Mr"], [2, "Fang", "Ms"], [3, "Elbert", "Dr"]]
        pdf = pd.DataFrame(data, columns=["date", "name", "title"])
    else:
        data = [[2, "Teo"], [2, "Fang"], [1, "Elia"]]
        pdf = pd.DataFrame(data, columns=["date", "name"])

    df = spark.createDataFrame(pdf)

    return df
