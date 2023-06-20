import pandas as pd  # type: ignore


def model(dbt, spark):
    dbt.config(materialized="incremental")
    dbt.config(partition_by="date")
    dbt.config(unique_key="name")
    data = [["2023-01-01", "Elia"], ["2023-01-02", "Teo"], ["2023-01-02", "Fang"]]

    pdf = pd.DataFrame(data, columns=["date", "name"])

    df = spark.createDataFrame(pdf)

    return df
