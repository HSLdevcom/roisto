# -*- coding: utf-8 -*-
"""Map PubTrans DatedVehicleJourney to Jore information."""

import functools
import logging

from roisto.match import common
from roisto.match import mapper

LOG = logging.getLogger(__name__)

_JOURNEY_QUERY = """
    SELECT
        CONVERT(CHAR(16), DVJ.Id) AS DatedVehicleJourneyId,
        KVV.StringValue AS JoreLineId,
        SUBSTRING(
            CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid),
            12,
            1
        ) AS JoreDirection,
        DATEADD(
            SECOND,
            DATEDIFF(
                SECOND,
                '1900-01-01',
                VJ.PlannedStartOffsetDatetime),
            DVJ.OperatingDayDate
        ) AS LocalizedStartTime
    FROM
        DatedVehicleJourney AS DVJ,
        VehicleJourney AS VJ,
        VehicleJourneyTemplate AS VJT,
        -- Publication.T and hence schema T go with VehicleJourney.
        T.KeyVariantValue AS KVV,
        KeyType AS KT,
        KeyVariantType AS KVT,
        ObjectType AS OT
    WHERE
        DVJ.IsBasedOnVehicleJourneyId = VJ.Id
        AND DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id

        -- Perhaps the KeyType.Name is more permanent than KeyType.Id.
        AND (
            KT.Name = 'JoreIdentity'
            OR KT.Name = 'JoreRouteIdentity'
            OR KT.Name = 'RouteName'
        )

        AND KT.ExtendsObjectTypeNumber = OT.Number
        -- Filter out other KeyTypes but one.
        -- Publication.T and hence schema T go with VehicleJourney.
        AND OT.Name = 'VehicleJourney'

        AND KT.Id = KVT.IsForKeyTypeId
        AND KVT.Id = KVV.IsOfKeyVariantTypeId
        -- Publication.T and hence schema T go with VehicleJourney.
        AND KVV.IsForObjectId = VJ.Id

        AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL

        AND DVJ.OperatingDayDate >= '{past_utc}'
        AND DVJ.OperatingDayDate < '{future_utc}'

        AND DVJ.IsReplacedById IS NULL
"""


async def _update_journeys(sql_connector):
    LOG.info('Updating journey mapping.')
    past_utc, future_utc = common.get_window()
    query = _JOURNEY_QUERY.format(past_utc=past_utc, future_utc=future_utc)
    LOG.debug('Querying journeys from ptDOI:%s', query)
    result = await sql_connector.query_from_doi(query)
    LOG.debug('Got %d journeys.', len(result))
    return result


def create_journey_mapper(sql_connector):
    """Create a Mapper from DatedVehicleJourney to Jore information."""
    update = functools.partial(_update_journeys, sql_connector)
    parse = functools.partial(common.parse_mapper_query_results,
                              'DatedVehicleJourneyId')
    return mapper.Mapper(update, parse)
