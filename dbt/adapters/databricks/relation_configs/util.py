from agate import Table, Row


def get_first_row(results: Table) -> Row:
    if len(results.rows) == 0:
        return Row(values=set())
    return results.rows[0]
