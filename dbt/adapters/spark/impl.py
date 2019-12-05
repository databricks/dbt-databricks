from agate import Column
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.spark import SparkRelation
from dbt.adapters.spark import SparkConnectionManager
import dbt.exceptions
from typing import List

from dbt.logger import GLOBAL_LOGGER as logger
import agate


LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
GET_RELATION_TYPE_MACRO_NAME = 'spark_get_relation_type'
DROP_RELATION_MACRO_NAME = 'drop_relation'


class SparkAdapter(SQLAdapter):
    ConnectionManager = SparkConnectionManager
    Relation = SparkRelation

    column_names = (
        'table_database',
        'table_schema',
        'table_name',
        'table_type',
        'table_comment',
        'table_owner',
        'column_name',
        'column_index',
        'column_type',
        'column_comment',
    )

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "STRING"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "DOUBLE" if decimals else "BIGINT"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "TIMESTAMP"

    def create_schema(self, database, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            'Schema/Database creation is not supported in the Spark adapter. '
            'Please create the database "{}" manually'.format(database)
        )

    def drop_schema(self, database, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            'Schema/Database deletion is not supported in the Spark adapter. '
            'Please drop the database "{}" manually'.format(database)
        )

    def get_relation_type(self, relation, model_name=None):
        kwargs = {'relation': relation}
        return self.execute_macro(
            GET_RELATION_TYPE_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name,
            release=True
        )

    # Override that creates macros without a known type - adapter macros that
    # require a type will dynamically check at query-time
    def list_relations_without_caching(self, information_schema, schema,
                                       model_name=None) -> List[Relation]:
        kwargs = {'information_schema': information_schema, 'schema': schema}
        results = self.execute_macro(
            LIST_RELATIONS_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name,
            release=True
        )

        relations = []
        quote_policy = {
            'schema': True,
            'identifier': True
        }
        for _database, name, _ in results:
            relations.append(self.Relation.create(
                database=_database,
                schema=_database,
                identifier=name,
                quote_policy=quote_policy,
                type=None
            ))
        return relations

    # Override that doesn't check the type of the relation -- we do it
    # dynamically in the macro code
    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)

        self.execute_macro(
            DROP_RELATION_MACRO_NAME,
            kwargs={'relation': relation},
            connection_name=model_name
        )

    @staticmethod
    def _parse_relation(relation: Relation, table_columns: List[Column], rel_type: str) -> List[dict]:
        table_owner = None
        found_detailed_table_marker = False
        for column in table_columns:
            if column.name == '# Detailed Table Information':
                found_detailed_table_marker = True

            # In case there is another column with the name Owner
            if not found_detailed_table_marker:
                continue

            if column.name == 'Owner':
                table_owner = column.data_type

        columns = []
        for column in table_columns:
            # Fixes for pseudocolumns with no type
            if column.name in {
                '# Partition Information',
                '# col_name',
                ''
            }:
                continue
            elif column.name == '# Detailed Table Information':
                # Loop until the detailed table information
                break
            elif column.dtype is None:
                continue

            column_data = (
                relation.database,
                relation.schema,
                relation.name,
                rel_type,
                None,
                table_owner,
                column.name,
                len(columns),
                column.data_type,
                None
            )
            column_dict = dict(zip(SparkAdapter.column_names, column_data))
            columns.append(column_dict)

        return columns

    def get_catalog(self, manifest):
        schemas = manifest.get_used_schemas()

        columns = []
        for (database_name, schema_name) in schemas:
            relations = self.list_relations(database_name, schema_name)
            for relation in relations:
                logger.debug("Getting table schema for relation {}".format(relation))  # noqa
                table_columns = self.get_columns_in_relation(relation)
                rel_type = self.get_relation_type(relation)
                columns += self._parse_relation(relation, table_columns, rel_type)

        return dbt.clients.agate_helper.table_from_data(columns, SparkAdapter.column_names)
