# -*- coding: utf-8 -*-
"""Map PubTrans DatedVehicleJourney to UTC offset in minutes."""

import functools
import logging

from roisto.match import common
from roisto.match import mapper

LOG = logging.getLogger(__name__)

_UTC_OFFSET_QUERY = """
    SELECT
        CONVERT(CHAR(16), Id) AS DatedVehicleJourneyId,
        UTCOffsetMinutes
    FROM
        DatedVehicleJourney
    WHERE
        OperatingDayDate >= '{past_utc}'
        AND OperatingDayDate < '{future_utc}'
"""


async def _update_utc_offsets(sql_connector):
    LOG.info('Updating UTC offset mapping.')
    past_utc, future_utc = common.get_window()
    query = _UTC_OFFSET_QUERY.format(past_utc=past_utc, future_utc=future_utc)
    LOG.debug('Querying UTC offsets from ptROI:%s', query)
    result = await sql_connector.query_from_roi(query)
    LOG.debug('Got %d UTC offsets.', len(result))
    return result


def create_utc_offset_mapper(sql_connector):
    """Create a Mapper from DatedVehicleJourney to UTC offset in minutes."""
    update = functools.partial(_update_utc_offsets, sql_connector)
    parse = functools.partial(common.parse_mapper_query_results,
                              'DatedVehicleJourneyId')
    return mapper.Mapper(update, parse)
