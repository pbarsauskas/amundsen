# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import namedtuple
from itertools import groupby
from typing import (
    Any, Dict, Iterator, Union,
)

from pyhocon import ConfigFactory, ConfigTree

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata

TableKey = namedtuple('TableKey', ['schema', 'table_name'])

LOGGER = logging.getLogger(__name__)


class ClickhouseMetadataExtractor(Extractor):
    """
    Extracts clickhouse table and column metadata from underlying meta store database using SQLAlchemyExtractor
    """
    # SELECT statement from clickhouse system database to extract table and column metadata
    SQL_STATEMENT = """
        SELECT
        name AS col_name,
        comment AS col_description,
        type AS col_type,
        position AS col_sort_order,
        '{cluster_source}' AS cluster,
        database,
        table,
        '' AS description
        FROM
        system.columns
        WHERE {where_clause_suffix}
        ORDER by cluster, database, name, col_sort_order;
    """

    # CONFIG KEYS
    WHERE_CLAUSE_SUFFIX_KEY = """database not in ('system')"""
    CLUSTER_KEY = 'production'
    DATABASE_KEY = 'clickhouse'

    # Default values
    DEFAULT_CLUSTER_NAME = 'production'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {WHERE_CLAUSE_SUFFIX_KEY: ' ', CLUSTER_KEY: DEFAULT_CLUSTER_NAME}
    )

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(ClickhouseMetadataExtractor.DEFAULT_CONFIG)
        self._cluster = conf.get_string(ClickhouseMetadataExtractor.CLUSTER_KEY)

        self._database = conf.get_string(ClickhouseMetadataExtractor.DATABASE_KEY, default='clickhouse')

        self.sql_stmt = ClickhouseMetadataExtractor.SQL_STATEMENT.format(
            where_clause_suffix=conf.get_string(ClickhouseMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY),
            cluster_source=conf.get_string(ClickhouseMetadataExtractor.CLUSTER_KEY)
        )

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope()) \
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self.sql_stmt = sql_alch_conf.get_string(SQLAlchemyExtractor.EXTRACT_SQL)

        LOGGER.info('SQL for clickhouse metadata: %s', self.sql_stmt)

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[TableMetadata, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.clickhouse_metadata'

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """
        for key, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                columns.append(ColumnMetadata(row['col_name'], row['col_description'],
                                              row['col_type'], row['col_sort_order']))

            yield TableMetadata(self._database, last_row['cluster'],
                                last_row['database'],
                                last_row['table'],
                                last_row['description'],
                                columns,
                                tags=last_row['database'])

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()

    def _get_table_key(self, row: Dict[str, Any]) -> Union[TableKey, None]:
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(schema=row['database'], table_name=row['table'])

        return None
