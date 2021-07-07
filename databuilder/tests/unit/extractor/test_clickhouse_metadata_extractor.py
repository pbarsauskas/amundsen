# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import unittest
from typing import Any, Dict

from mock import MagicMock, patch
from pyhocon import ConfigFactory

from databuilder.extractor.clickhouse_metadata_extractor import ClickhouseMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata


class TestClickhouseMetadataExtractor(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            f'extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': 'test_connection',
            f'extractor.clickhouse_metadata.{ClickhouseMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': ' 1=1 ',
            f'extractor.clickhouse_metadata.{ClickhouseMetadataExtractor.CLUSTER_KEY}': 'test_cluster',
            f'extractor.clickhouse_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': 'conn_string',
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extraction_with_empty_query_result(self) -> None:
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, '_get_connection'):
            extractor = ClickhouseMetadataExtractor()
            extractor.init(self.conf)

            results = extractor.extract()
            self.assertEqual(results, None)

    def test_extraction_with_single_result(self) -> None:
        with patch.object(SQLAlchemyExtractor, '_get_connection') as mock_connection:
            connection = MagicMock()
            mock_connection.return_value = connection
            sql_execute = MagicMock()
            connection.execute = sql_execute
            table = {'database': 'default',
                     'table': 'columns',
                     'description': 'a table for testing',
                     'cluster':
                         self.conf[f'extractor.clickhouse_metadata.{ClickhouseMetadataExtractor.CLUSTER_KEY}'],
                     'extract_date': '2021-08-01'
                     }

            sql_execute.return_value = [
                self._union(
                    {'col_name': 'col_id1',
                     'col_type': 'bigint',
                     'col_description': 'description of id1',
                     'col_sort_order': 0}, table),
                self._union(
                    {'col_name': 'col_id2',
                     'col_type': 'bigint',
                     'col_description': 'description of id2',
                     'col_sort_order': 1}, table),
                self._union(
                    {'col_name': 'is_active',
                     'col_type': 'boolean',
                     'col_description': None,
                     'col_sort_order': 2}, table),
                self._union(
                    {'col_name': 'source',
                     'col_type': 'varchar',
                     'col_description': 'description of source',
                     'col_sort_order': 3}, table),
                self._union(
                    {'col_name': 'etl_created_at',
                     'col_type': 'timestamp',
                     'col_description': 'description of etl_created_at',
                     'col_sort_order': 4}, table),
                self._union(
                    {'col_name': 'ds',
                     'col_type': 'varchar',
                     'col_description': None,
                     'col_sort_order': 5}, table)
            ]

            extractor = ClickhouseMetadataExtractor()
            extractor.init(self.conf)
            actual = extractor.extract()
            expected = TableMetadata('clickhouse', 'test_cluster', 'default', 'columns', 'a table for testing',
                                     [ColumnMetadata('col_id1', 'description of id1', 'bigint', 0),
                                      ColumnMetadata('col_id2', 'description of id2', 'bigint', 1),
                                      ColumnMetadata('is_active', None, 'boolean', 2),
                                      ColumnMetadata('source', 'description of source', 'varchar', 3),
                                      ColumnMetadata('etl_created_at', 'description of etl_created_at', 'timestamp', 4),
                                      ColumnMetadata('ds', None, 'varchar', 5)], tags='default')

            self.assertEqual(expected.__repr__(), actual.__repr__())
            self.assertIsNone(extractor.extract())

    def _union(self,
               target: Dict[Any, Any],
               extra: Dict[Any, Any]) -> Dict[Any, Any]:
        target.update(extra)
        return target


class TestClickhouseMetadataExtractorWithWhereClause(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)
        self.where_clause_suffix = """
            1=1
        """

        config_dict = {
            ClickhouseMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY: self.where_clause_suffix,
            f'extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': 'test_connection'
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_sql_statement(self) -> None:
        """
        Test Extraction with empty result from query
        """
        with patch.object(SQLAlchemyExtractor, '_get_connection'):
            extractor = ClickhouseMetadataExtractor()
            extractor.init(self.conf)
            self.assertTrue(self.where_clause_suffix in extractor.sql_stmt)


if __name__ == '__main__':
    unittest.main()
