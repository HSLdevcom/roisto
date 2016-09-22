# -*- coding: utf-8 -*-
"""Poll the PubTrans SQL database."""

import datetime
import functools
import json
import logging

import pymssql

from roisto import util

LOG = logging.getLogger(__name__)

MINUTES_IN_HOUR = 60


def _minutes_to_hours_string(minutes):
    sign = '+'
    if minutes < 0:
        sign = '-'
    hours, minutes_left = divmod(abs(minutes), MINUTES_IN_HOUR)
    return '{sign}{hours:02d}:{minutes:02d}'.format(
        sign=sign, hours=hours, minutes=minutes_left)


def _format_datetime_for_sql(dt):
    return dt.strftime('%Y%m%d %H:%M:%S.') + dt.strftime('%f')[:3]


def _combine_into_timestamp(naive_datetime, utc_offset_minutes):
    naive_string = naive_datetime.isoformat()
    if '.' in naive_string:
        # Do not show more than milliseconds.
        naive_string = naive_string[:-3]
    return naive_string + _minutes_to_hours_string(utc_offset_minutes)


def _connect_and_query_synchronously(connect, query):
    """Connect and query once in synchronous code."""
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()


def _timestamp_day_shift(now, days):
    then = now + datetime.timedelta(days=days)
    return then.strftime('%Y-%m-%d')


def _create_timestamp():
    return _combine_into_timestamp(datetime.datetime.utcnow(), 0)


class PredictionFilter:
    """Filter out already handled predictions."""

    def __init__(self):
        # The modification time on the latest record in the rows given to
        # update(). Initial value does not matter as long as its a naive
        # datetime.
        self._max_modification = datetime.datetime.utcnow()
        # A set of Arrival.Id values where the modification time matches
        # self._max_modification.
        self._arrival_ids = set()

    def update(self, rows):
        """Find relevant values for filtering."""
        if rows:
            self._max_modification = max(row[6] for row in rows)
            self._arrival_ids = {row[0]
                                 for row in rows
                                 if row[6] == self._max_modification}
        else:
            LOG.error('PredictionFilter.update() must be called with a '
                      'non-empty sequence.')

    def filter(self, rows):
        """Filter out already handled rows.

        Note that at least on 2016-09-13 the Microsoft SQL Server data type
        datetime has less than a millisecond precision:
        https://msdn.microsoft.com/en-us/library/ms187819.aspx .

        Hopefully the comparison of values with that data type is consistent,
        e.g.
        01/01/98 13:59:59.995 == 01/01/98 13:59:59.998
        is expected to be true for datetime in SQL Server.
        """
        fresh = [row for row in rows
                 if not (row[6] == self._max_modification and row[0] in
                         self._arrival_ids)]
        return fresh

    def get_latest_modification_datetime(self):
        """Return the modification time on the latest known record."""
        return self._max_modification


class PredictionPoller:
    """Poll for predictions and forward MONO messages.

    PredictionPoller also queries for information that allows mapping PubTrans
    IDs to Jore IDs.
    """

    _AT_LEAST_DAYS_BACK_SHIFT = -1
    _AT_MOST_DAYS_FORWARD_SHIFT = 2
    PREDICTION_QUERY = """
        SELECT
            CONVERT(CHAR(16), A.Id) AS ArrivalId,
            CONVERT(CHAR(16), DVJ.Id
            ) AS DatedVehicleJourneyId,
            CONCAT(
                CONVERT(CHAR(8), DVJ.OperatingDayDate, 112),
                ':',
                CONVERT(CHAR(16), DVJ.Gid)
            ) AS DatedVehicleJourneyUniqueGid,
            CONVERT(CHAR(16), A.IsTargetedAtJourneyPatternPointGid
            ) AS JourneyPatternPointGid,
            A.TimetabledLatestDateTime,
            A.EstimatedDateTime,
            A.LastModifiedUTCDateTime
        FROM
            Arrival AS A,
            DatedVehicleJourney AS DVJ
        WHERE
            A.IsOnDatedVehicleJourneyId = DVJ.Id
            AND DVJ.IsOnDirectionOfLineGid IS NOT NULL
            AND A.LastModifiedUTCDateTime >= '{modified_utc}'
            AND A.EstimatedDateTime IS NOT NULL
            AND A.LastModifiedUTCDateTime IS NOT NULL
    """
    STOP_QUERY = """
        SELECT
            CONVERT(CHAR(16), Gid) AS JourneyPatternPointGid,
            CONVERT(CHAR(7), Number) AS JoreStopId
        FROM
            JourneyPatternPoint
    """
    JOURNEY_QUERY = """
        SELECT
            CONVERT(CHAR(16), DVJ.Id) AS DatedVehicleJourneyId,
            CONCAT(
                CONVERT(CHAR(8), DVJ.OperatingDayDate, 112),
                ':',
                CONVERT(CHAR(16), DVJ.Gid)
            ) AS DatedVehicleJourneyUniqueGid,
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
    """
    UTC_OFFSET_QUERY = """
        SELECT
            Id AS DatedVehicleJourneyId,
            CONCAT(
                CONVERT(CHAR(8), OperatingDayDate, 112),
                ':',
                CONVERT(CHAR(16), Gid)
            ) AS DatedVehicleJourneyUniqueGid,
            UTCOffsetMinutes
        FROM
            DatedVehicleJourney
        WHERE
            OperatingDayDate >= '{past_utc}'
            AND OperatingDayDate < '{future_utc}'
    """

    def __init__(self, config, async_helper, queue, is_mqtt_connected):
        self._async_helper = async_helper
        self._queue = queue
        self._is_mqtt_connected = is_mqtt_connected

        # Connecting functions.
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

        self._prediction_poll_sleep_in_seconds = util.convert_duration_to_seconds(
            config['prediction_poll_sleep'])

        # Dictionaries for matching PubTrans IDs to Jore IDs.
        self._stops = {}
        self._journeys = {}

        # Accounting for the timezone.
        self._utc_offsets = {}

    async def _connect_and_query(self, connect, query):
        """Connect and query once.

        pymssql supports only one cursor per connection so several simultaneous
        queries require several connections.
        """
        result = []
        try:
            result = await self._async_helper.run_in_executor(
                _connect_and_query_synchronously, connect, query)
        except pymssql.Error as ex:
            LOG.warning('SQL error: %s', str(ex))
        except pymssql.Warning as ex:
            LOG.warning('SQL warning: %s', str(ex))
        return result

    async def _update_stops(self):
        query = PredictionPoller.STOP_QUERY
        LOG.debug('Querying stops from DOI:%s', query)
        result = await self._connect_and_query(self._doi_connect, query)
        if result:
            LOG.debug('Got %d stops.', len(result))
            self._stops = dict(result)

    async def _update_journeys(self):
        now = datetime.datetime.utcnow()
        past_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_LEAST_DAYS_BACK_SHIFT)
        future_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_MOST_DAYS_FORWARD_SHIFT)
        query = PredictionPoller.JOURNEY_QUERY.format(
            past_utc=past_utc, future_utc=future_utc)
        LOG.debug('Querying journeys from DOI:%s', query)
        result = await self._connect_and_query(self._doi_connect, query)
        if result:
            LOG.debug('Got %d journeys.', len(result))
            rearranged = {}
            for row in result:
                rearranged[row[1]] = {
                    'JoreLineId': row[2],
                    'JoreDirection': row[3],
                    'LocalizedStartTime': row[4],
                }
            self._journeys = rearranged

    async def _update_utc_offsets(self):
        now = datetime.datetime.utcnow()
        past_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_LEAST_DAYS_BACK_SHIFT)
        future_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_MOST_DAYS_FORWARD_SHIFT)
        query = PredictionPoller.UTC_OFFSET_QUERY.format(
            past_utc=past_utc, future_utc=future_utc)
        LOG.debug('Querying UTC offsets from ROI:%s', query)
        result = await self._connect_and_query(self._roi_connect, query)
        if result:
            LOG.debug('Got %d UTC offsets.', len(result))
            self._utc_offsets = {row[1]: row[2] for row in result}

    async def _update_jore_mapping(self):
        tasks = [
            self._update_stops(),
            self._update_journeys(),
            self._update_utc_offsets(),
        ]
        return await self._async_helper.wait(tasks)

    def _gather_predictions_per_stop(self, result):
        predictions_by_stop = {}
        for row in result:
            dvj = row[2]
            journey_info = self._journeys.get(dvj, None)
            if journey_info is None:
                LOG.warning('This DatedVehicleJourneyUniqueGid was not found '
                            'from collected journey information: %s. '
                            'Prediction row was: %s', dvj, str(row))
                return None
            utc_offset = self._utc_offsets.get(dvj, None)
            if utc_offset is None:
                LOG.warning('This DatedVehicleJourneyUniqueGid was not found '
                            'from collected UTC offset information: %s.'
                            'Prediction row was: %s', dvj, str(row))
                return None
            jpp = row[3]
            stop = self._stops.get(jpp, None)
            if stop is None:
                LOG.warning('This JourneyPatternPointGid was not found from '
                            'collected stop information: %s. Prediction row '
                            'was: %s', jpp, str(row))
                return None
            start_naive = journey_info['LocalizedStartTime']
            scheduled_naive = row[4]
            predicted_naive = row[5]
            start_time = _combine_into_timestamp(start_naive, utc_offset)
            scheduled_time = _combine_into_timestamp(scheduled_naive,
                                                     utc_offset)
            predicted_time = _combine_into_timestamp(predicted_naive,
                                                     utc_offset)
            prediction = {
                'joreStopId': stop,
                'joreLineId': journey_info['JoreLineId'],
                'joreLineDirection': journey_info['JoreDirection'],
                'journeyStartTime': start_time,
                'scheduledArrivalTime': scheduled_time,
                'predictedArrivalTime': predicted_time,
            }
            predictions_list = predictions_by_stop.get(stop, [])
            predictions_list.append(prediction)
            predictions_by_stop[stop] = predictions_list
        return predictions_by_stop

    async def _keep_polling_predictions(self):
        prediction_filter = PredictionFilter()
        modified_utc_dt = (datetime.datetime.utcnow() - datetime.timedelta(
            seconds=self._prediction_poll_sleep_in_seconds))
        while True:
            modified_utc = _format_datetime_for_sql(modified_utc_dt)
            query = PredictionPoller.PREDICTION_QUERY.format(
                modified_utc=modified_utc)
            LOG.debug('Starting to wait for MQTT connection.')
            await self._async_helper.wait_for_event(self._is_mqtt_connected)
            LOG.debug('Querying predictions from ROI:%s', query)
            result = await self._connect_and_query(self._roi_connect, query)
            old_len = len(result)
            result = prediction_filter.filter(result)
            new_len = len(result)
            LOG.debug('Got %s predictions of which %s were new.', old_len,
                      new_len)
            if result:
                message_timestamp = _create_timestamp()
                predictions_by_stop = self._gather_predictions_per_stop(result)
                if predictions_by_stop is None:
                    LOG.info('Jore mapping was insufficient so start to '
                             'update it.')
                    await self._update_jore_mapping()
                    # This is rare so let's just query the same predictions
                    # again.
                else:
                    for stop_id, predictions in predictions_by_stop.items():
                        topic_suffix = stop_id
                        message = {
                            'messageTimestamp': message_timestamp,
                            'predictions': predictions,
                        }
                        await self._queue.put(
                            (topic_suffix, json.dumps(message)))
                    prediction_filter.update(result)
                    modified_utc_dt = prediction_filter.get_latest_modification_datetime()
            await self._async_helper.sleep(
                self._prediction_poll_sleep_in_seconds)

    async def run(self):
        """Run the PredictionPoller."""
        LOG.debug('Starting to poll predictions.')
        await self._keep_polling_predictions()
        LOG.error('Prediction polling ended unexpectedly.')
