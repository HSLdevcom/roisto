# -*- coding: utf-8 -*-
"""Map PubTrans JourneyPatternPoint to Jore stop."""

import functools
import logging

from roisto.match import common
from roisto.match import mapper

LOG = logging.getLogger(__name__)

# xx99xxx should refer to via points. Mono does not care about them, so
# avoid extra burden.
#
# ExistsFromDate and ExistsUptoDate do not matter as there is 1:1
# correspondence between Gid and Number, even in different versions of same
# stop.
_STOP_QUERY = """
    SELECT DISTINCT
        CONVERT(CHAR(16), Gid) AS JourneyPatternPointGid,
        CONVERT(CHAR(7), Number) AS JoreStopId
    FROM
        JourneyPatternPoint
    WHERE
        Number % 100000 < 99000
"""


async def _update_stops(sql_connector):
    LOG.info('Updating stop mapping.')
    query = _STOP_QUERY
    LOG.debug('Querying stops from ptDOI:%s', query)
    result = await sql_connector.query_from_doi(query)
    LOG.debug('Got %d stops.', len(result))
    return result


def create_stop_mapper(sql_connector):
    """Create a Mapper from JourneyPatternPointGid to Jore stop."""
    update = functools.partial(_update_stops, sql_connector)
    parse = functools.partial(common.parse_mapper_query_results,
                              'JourneyPatternPointGid')
    return mapper.Mapper(update, parse)
