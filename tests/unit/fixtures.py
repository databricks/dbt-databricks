from typing import List

from agate import Table


def gen_describe_extended(
    columns: List[List[str]] = [["col_a", "int", "This is a comment"]],
    partition_info: List[List[str]] = [],
    detailed_table_info: List[List[str]] = [],
) -> Table:
    return Table(
        rows=[
            ["col_name", "data_type", "comment"],
            *columns,
            [None, None, None],
            ["# Partition Information", None, None],
            ["# col_name", "data_type", "comment"],
            *partition_info,
            [None, None, None],
            ["# Detailed Table Information", None, None],
            *detailed_table_info,
        ],
        column_names=["col_name", "data_type", "comment"],
    )


def gen_tblproperties(rows: List[List[str]] = [["prop", "1"], ["other", "other"]]) -> Table:
    return Table(rows=rows, column_names=["key", "value"])
