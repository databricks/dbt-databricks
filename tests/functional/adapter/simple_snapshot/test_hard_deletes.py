"""
Tests for hard_deletes configuration in snapshots.

Tests all three hard_deletes modes:
- ignore: deleted records are not tracked in snapshot
- invalidate: deleted records have dbt_valid_to set
- new_record: deleted records get new row with dbt_is_deleted=True
"""

import pytest

from dbt.tests.util import run_dbt

# Snapshot SQL templates for each hard_deletes mode
snapshot_sql_ignore = """
{% snapshot snapshot_hard_delete_ignore %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['name', 'city'],
            hard_deletes='ignore',
        )
    }}
    select * from {{ schema }}.seed_hard_delete
{% endsnapshot %}
"""

snapshot_sql_invalidate = """
{% snapshot snapshot_hard_delete_invalidate %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['name', 'city'],
            hard_deletes='invalidate',
        )
    }}
    select * from {{ schema }}.seed_hard_delete
{% endsnapshot %}
"""

snapshot_sql_new_record = """
{% snapshot snapshot_hard_delete_new_record %}
    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['name', 'city'],
            hard_deletes='new_record',
        )
    }}
    select * from {{ schema }}.seed_hard_delete
{% endsnapshot %}
"""


class BaseHardDeleteTest:
    """Base class for hard delete tests"""

    def setup_initial_data(self, project):
        """Create initial seed data with 5 records"""
        create_seed_sql = f"""
            create table {project.test_schema}.seed_hard_delete (
                id integer,
                name string,
                city string,
                updated_at timestamp
            )
        """
        project.run_sql(create_seed_sql)

        insert_seed_sql = f"""
            insert into {project.test_schema}.seed_hard_delete (id, name, city, updated_at) values
            (1, 'Alice', 'London', current_timestamp()),
            (2, 'Bob', 'Paris', current_timestamp()),
            (3, 'Charlie', 'Berlin', current_timestamp()),
            (4, 'Diana', 'Madrid', current_timestamp()),
            (5, 'Eve', 'Rome', current_timestamp())
        """
        project.run_sql(insert_seed_sql)

    def delete_records(self, project, ids_to_delete):
        """Delete specific records from seed table"""
        ids_str = ",".join(str(id) for id in ids_to_delete)
        delete_sql = f"""
            delete from {project.test_schema}.seed_hard_delete where id in ({ids_str})
        """
        project.run_sql(delete_sql)

    def get_snapshot_records(self, project, snapshot_name):
        """Get all records from snapshot table"""
        query = f"select * from {project.test_schema}.{snapshot_name} order by id, dbt_valid_from"
        return project.run_sql(query, fetch="all")

    def count_records_by_id(self, project, snapshot_name, record_id):
        """Count how many snapshot records exist for a given id"""
        query = f"""
            select count(*) from {project.test_schema}.{snapshot_name}
            where id = {record_id}
        """
        result = project.run_sql(query, fetch="one")
        return result[0]


class TestHardDeleteIgnore(BaseHardDeleteTest):
    """Test hard_deletes='ignore' mode"""

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_hard_delete_ignore.sql": snapshot_sql_ignore}

    def test_hard_delete_ignore(self, project):
        """
        Test that with hard_deletes='ignore', deleted records remain unchanged in snapshot.

        Expected behavior:
        - After deletion, snapshot should still contain all original records
        - No new records should be added
        - dbt_valid_to should remain NULL for deleted records
        """
        # Setup initial data
        self.setup_initial_data(project)

        # Run initial snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Verify initial snapshot has 5 records
        initial_records = self.get_snapshot_records(project, "snapshot_hard_delete_ignore")
        assert len(initial_records) == 5

        # Delete records 3 and 4 from source
        self.delete_records(project, [3, 4])

        # Run snapshot again
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # With 'ignore', snapshot should still have 5 records
        # (no change - deleted records remain in snapshot)
        final_records = self.get_snapshot_records(project, "snapshot_hard_delete_ignore")
        assert len(final_records) == 5, (
            f"Expected 5 records with hard_deletes='ignore', got {len(final_records)}. "
            "Deleted records should remain unchanged in snapshot."
        )

        # Verify deleted records (ids 3 and 4) still have NULL dbt_valid_to
        # Snapshot columns: id, name, city, updated_at, dbt_scd_id,
        # dbt_updated_at, dbt_valid_from, dbt_valid_to
        # dbt_valid_to is the last column (index -1)
        deleted_ids_found = []
        for record in final_records:
            if record[0] in [3, 4]:  # id is first column
                deleted_ids_found.append(record[0])
                dbt_valid_to = record[-1]  # last column
                assert dbt_valid_to is None, (
                    f"Record id={record[0]} should have NULL dbt_valid_to "
                    f"with hard_deletes='ignore', but got: {dbt_valid_to}"
                )

        assert len(deleted_ids_found) == 2, (
            f"Should find both deleted records (3 and 4) in snapshot with hard_deletes='ignore', "
            f"but found: {deleted_ids_found}"
        )


class TestHardDeleteInvalidate(BaseHardDeleteTest):
    """Test hard_deletes='invalidate' mode"""

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_hard_delete_invalidate.sql": snapshot_sql_invalidate}

    def test_hard_delete_invalidate(self, project):
        """
        Test that with hard_deletes='invalidate', deleted records have dbt_valid_to set.

        Expected behavior:
        - Deleted records should have dbt_valid_to set to a timestamp
        - No new records should be added
        - Total record count remains the same
        """
        # Setup initial data
        self.setup_initial_data(project)

        # Run initial snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Verify initial snapshot has 5 records
        initial_records = self.get_snapshot_records(project, "snapshot_hard_delete_invalidate")
        assert len(initial_records) == 5

        # Delete records 3 and 4
        self.delete_records(project, [3, 4])

        # Run snapshot again
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # With 'invalidate', snapshot should still have 5 records
        final_records = self.get_snapshot_records(project, "snapshot_hard_delete_invalidate")
        assert (
            len(final_records) == 5
        ), f"Expected 5 records with hard_deletes='invalidate', got {len(final_records)}"

        # Verify deleted records (3, 4) have dbt_valid_to set (not NULL)
        # Snapshot columns: id, name, city, updated_at, dbt_scd_id,
        # dbt_updated_at, dbt_valid_from, dbt_valid_to
        # dbt_valid_to is the last column (index -1)
        invalidated_count = 0
        for record in final_records:
            if record[0] in [3, 4]:  # id column
                # dbt_valid_to should NOT be NULL
                dbt_valid_to = record[-1]  # last column
                assert dbt_valid_to is not None, (
                    f"Record id={record[0]} should have dbt_valid_to set "
                    f"with hard_deletes='invalidate', but got {dbt_valid_to}"
                )
                invalidated_count += 1

        assert invalidated_count == 2, f"Expected 2 invalidated records, found {invalidated_count}"


class TestHardDeleteNewRecord(BaseHardDeleteTest):
    """Test hard_deletes='new_record' mode"""

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot_hard_delete_new_record.sql": snapshot_sql_new_record}

    def test_hard_delete_new_record(self, project):
        """
        Test that with hard_deletes='new_record', deleted records get new rows
        with dbt_is_deleted=True.

        Expected behavior:
        - Original records should have dbt_valid_to set
        - New records should be inserted with dbt_is_deleted=True
        - Total record count increases by number of deleted records
        """
        # Setup initial data
        self.setup_initial_data(project)

        # Run initial snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Verify initial snapshot has 5 records
        initial_records = self.get_snapshot_records(project, "snapshot_hard_delete_new_record")
        assert len(initial_records) == 5

        # Delete records 3 and 4
        self.delete_records(project, [3, 4])

        # Run snapshot again
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # With 'new_record', snapshot should have 7 records (5 original + 2 new deletion records)
        final_records = self.get_snapshot_records(project, "snapshot_hard_delete_new_record")
        assert len(final_records) == 7, (
            f"Expected 7 records with hard_deletes='new_record' (5 original + 2 deletion records), "
            f"got {len(final_records)}"
        )

        # Verify we have 2 records for each deleted id (3 and 4)
        count_id_3 = self.count_records_by_id(project, "snapshot_hard_delete_new_record", 3)
        count_id_4 = self.count_records_by_id(project, "snapshot_hard_delete_new_record", 4)

        assert count_id_3 == 2, f"Expected 2 records for id=3, got {count_id_3}"
        assert count_id_4 == 2, f"Expected 2 records for id=4, got {count_id_4}"

        # Check for dbt_is_deleted column existence and values
        # Note: This requires checking if the column exists in the snapshot
        check_deleted_sql = f"""
            select count(*) from {project.test_schema}.snapshot_hard_delete_new_record
            where dbt_is_deleted = true and id in (3, 4)
        """

        try:
            deleted_records = project.run_sql(check_deleted_sql, fetch="one")
            assert deleted_records[0] == 2, (
                f"Expected 2 records with dbt_is_deleted=true for ids 3 and 4, "
                f"got {deleted_records[0]}"
            )
        except Exception as e:
            # If dbt_is_deleted column doesn't exist, the test should fail
            pytest.fail(
                f"dbt_is_deleted column should exist with hard_deletes='new_record'. Error: {e}"
            )
