# -*- coding: utf-8 -*-
"""Connect to and query the SQL database."""

import functools
import logging

import pymssql

LOG = logging.getLogger(__name__)


def _connect_and_query_synchronously(connect, query):
    """Connect and query once in synchronous code."""
    with connect() as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(query)
            return cursor.fetchall()


class SQLConnector:
    """Connect to and query the SQL database."""

    def __init__(self, config, loop):
        self._loop = loop

        self._connect = functools.partial(
            pymssql.connect,
            server=config['host'],
            user=config['username'],
            password=config['password'],
            port=config['port'])
        self._doi_connect = functools.partial(
            self._connect, database=config['doi_database'])
        self._roi_connect = functools.partial(
            self._connect, database=config['roi_database'])

    async def _connect_and_query(self, connect, query):
        """Connect and query once.

        pymssql supports only one cursor per connection so several simultaneous
        queries require several connections.
        """
        result = []
        try:
            result = await self._loop.run_in_executor(
                None, _connect_and_query_synchronously, connect, query)
        except pymssql.Error as ex:
            LOG.warning('SQL error: %s', str(ex))
        except pymssql.Warning as ex:
            LOG.warning('SQL warning: %s', str(ex))
        return result

    def query_from_roi(self, query):
        """Query from ptROI."""
        return self._connect_and_query(self._roi_connect, query)

    def query_from_doi(self, query):
        """Query from ptDOI."""
        return self._connect_and_query(self._doi_connect, query)
