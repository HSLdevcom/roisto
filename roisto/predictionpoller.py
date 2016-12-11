# -*- coding: utf-8 -*-
"""Poll the PubTrans SQL database."""

import collections
import datetime
import functools
import json
import logging
import operator

import cachetools
import pymssql

from roisto import util
from roisto import mapper

LOG = logging.getLogger(__name__)

MINUTES_IN_HOUR = 60

# FIXME: Use rda* tables when they are ready. Meanwhile, use numeric values.
DEPARTURE_STATES = {
    0: 'NOTEXPECTED',
    1: 'NOTCALLED',
    2: 'EXPECTED',
    3: 'CANCELLED',
    4: 'INHIBITED',
    6: 'ATSTOP',
    7: 'BOARDING',
    8: 'BOARDINGCLOSED',
    9: 'DEPARTED',
    10: 'PASSED',
    11: 'MISSED',
    12: 'REPLACED',
    13: 'ASSUMEDDEPARTED',
}


def _minutes_to_hours_string(minutes):
    sign = '+'
    if minutes < 0:
        sign = '-'
    hours, minutes_left = divmod(abs(minutes), MINUTES_IN_HOUR)
    return '{sign}{hours:02d}:{minutes:02d}'.format(
        sign=sign, hours=hours, minutes=minutes_left)


def _combine_into_timestamp(naive_datetime, utc_offset_minutes):
    naive_string = naive_datetime.isoformat()
    if '.' in naive_string:
        # Do not show more than milliseconds.
        naive_string = naive_string[:-3]
    return naive_string + _minutes_to_hours_string(utc_offset_minutes)


def _create_timestamp():
    return _combine_into_timestamp(datetime.datetime.utcnow(), 0)


def _format_datetime_for_sql(dt):
    return dt.strftime('%Y%m%d %H:%M:%S.') + dt.strftime('%f')[:3]


def _connect_and_query_synchronously(connect, query):
    """Connect and query once in synchronous code."""
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()


def _timestamp_day_shift(now, days):
    then = now + datetime.timedelta(days=days)
    return then.strftime('%Y-%m-%d')


def _parse_stops(rows):
    return dict(rows)


def _parse_journeys(rows):
    return {
        row[0]: {
            'JoreLineId': row[1],
            'JoreDirection': row[2],
            'LocalizedStartTime': row[3],
        }
        for row in rows
    }


def _parse_utc_offsets(rows):
    return {row[0]: row[1] for row in rows}


def _arrange_prediction(row, match):
    stop = match['stop']
    journey_info = match['journey_info']
    utc_offset = match['utc_offset']

    start_naive = journey_info['LocalizedStartTime']
    scheduled_naive = row[3]
    predicted_naive = row[4]
    start_time = _combine_into_timestamp(start_naive, utc_offset)
    scheduled_time = _combine_into_timestamp(scheduled_naive, utc_offset)
    predicted_time = _combine_into_timestamp(predicted_naive, utc_offset)
    prediction = {
        'joreStopId': stop,
        'joreLineId': journey_info['JoreLineId'],
        'joreLineDirection': journey_info['JoreDirection'],
        'journeyStartTime': start_time,
        'scheduledArrivalTime': scheduled_time,
        'predictedArrivalTime': predicted_time,
    }
    return (stop, prediction)


def _arrange_predictions_by_stop(rows, matches):
    predictions_by_stop = collections.defaultdict(list)
    for (row, match) in zip(rows, matches):
        if match is None:
            LOG.debug('Match is not available for prediction row: %s',
                      str(row))
        else:
            stop, prediction = _arrange_prediction(row, match)
            predictions_by_stop[stop].append(prediction)
    return dict(predictions_by_stop)


def _arrange_event(row, match):
    stop = match['stop']
    journey_info = match['journey_info']
    utc_offset = match['utc_offset']

    start_naive = journey_info['LocalizedStartTime']
    scheduled_naive = row[3]
    event = DEPARTURE_STATES[row[4]]
    start_time = _combine_into_timestamp(start_naive, utc_offset)
    scheduled_time = _combine_into_timestamp(scheduled_naive, utc_offset)
    prediction = {
        'joreStopId': stop,
        'joreLineId': journey_info['JoreLineId'],
        'joreLineDirection': journey_info['JoreDirection'],
        'journeyStartTime': start_time,
        'scheduledArrivalTime': scheduled_time,
        'event': event,
    }
    return (stop, prediction)


def _arrange_events_by_stop(rows, matches):
    events_by_stop = collections.defaultdict(list)
    for (row, match) in zip(rows, matches):
        if match is None:
            LOG.debug('Match is not available for event row: %s', str(row))
        else:
            stop, event = _arrange_event(row, match)
            events_by_stop[stop].append(event)
    return dict(events_by_stop)


def _create_filter(cache_size, extract, check_for_change):
    cache = cachetools.LRUCache(maxsize=cache_size)

    def is_changed(row):
        """Check whether a value has changed enough."""
        key, current = extract(row)
        is_changed = True
        cached = cache.get(key, None)
        if cached is not None:
            is_changed = check_for_change(current, cached)
        if is_changed:
            cache[key] = current
        return is_changed

    def filter_out_unchanged(rows):
        """Filter out rows that have not changed enough since last call."""
        return list(filter(is_changed, rows))

    return filter_out_unchanged


def _extract_departure_id_and_event(row):
    # FIXME: Check after implementation of query.
    departure_id = row[0]
    event = row[4]
    return departure_id, event


def _check_prediction_for_change(threshold_in_s, current, old):
    return abs((current - old).total_seconds()) >= threshold_in_s


def _extract_arrival_id_and_prediction(row):
    arrival_id = row[0]
    prediction = row[4]
    return arrival_id, prediction


class PredictionFilter:
    """Filter out already handled predictions."""

    def __init__(self):
        # The modification time on the latest record in the rows given to
        # update(). Initial value does not matter as long as its a naive
        # datetime.
        self._max_modification = datetime.datetime.utcnow()
        # A set of Arrival.Id values where the modification time matches
        # self._max_modification.
        self._arrival_ids_modified_latest = set()

    def update(self, rows):
        """Find relevant values for filtering."""
        if rows:
            self._max_modification = max(row[5] for row in rows)
            self._arrival_ids_modified_latest = {
                row[0]
                for row in rows if row[5] == self._max_modification
            }
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
        fresh = [
            row for row in rows
            if not (row[5] == self._max_modification and row[0] in
                    self._arrival_ids_modified_latest)
        ]
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
    # At 2016-10-26T12:32Z it holds for every row of JourneyPatternPoint in
    # ptDOI that:
    # Gid % 10000000 = Number
    # So cut off via points that way.
    PREDICTION_QUERY = """
        SELECT
            CONVERT(CHAR(16), A.Id) AS ArrivalId,
            CONVERT(CHAR(16), A.IsOnDatedVehicleJourneyId) AS DatedVehicleJourneyId,
            CONVERT(CHAR(16), A.IsTargetedAtJourneyPatternPointGid
            ) AS JourneyPatternPointGid,
            A.TimetabledLatestDateTime,
            A.EstimatedDateTime,
            A.LastModifiedUTCDateTime
        FROM
            Arrival AS A
        WHERE
            A.LastModifiedUTCDateTime >= '{modified_utc}'
            AND A.EstimatedDateTime IS NOT NULL
            AND A.LastModifiedUTCDateTime IS NOT NULL
            AND (
                A.IsTargetedAtJourneyPatternPointGid % 10000000 < 1999000
                OR A.IsTargetedAtJourneyPatternPointGid % 10000000 > 1999999
            )
    """
    # Cut off via points.
    #
    # FIXME: Use rda* tables when they are ready. Meanwhile, use numeric
    #        values.
    # FIXME: To speed things up, do not join with DatedVehicleJourney. We do
    # get extra events then, though.
    EVENT_QUERY = """
        SELECT
            CONVERT(CHAR(16), D.Id) AS DepartureId,
            CONVERT(CHAR(16), D.IsOnDatedVehicleJourneyId) AS DatedVehicleJourneyId,
            CONVERT(CHAR(16), D.IsTargetedAtJourneyPatternPointGid
            ) AS JourneyPatternPointGid,
            D.TimetabledEarliestDateTime,
            D.State,
            D.LastModifiedUTCDateTime
        FROM
            Departure AS D
        WHERE
            D.State IN (9, 10, 11)
            AND D.LastModifiedUTCDateTime >= '{modified_utc}'
            AND D.LastModifiedUTCDateTime IS NOT NULL
            AND (
                D.IsTargetedAtJourneyPatternPointGid % 10000000 < 1999000
                OR D.IsTargetedAtJourneyPatternPointGid % 10000000 > 1999999
            )
    """
    # 1999xxx should refer to via points. Mono does not care about them, so
    # avoid extra burden.
    #
    # ExistsFromDate and ExistsUptoDate do not matter as there is 1:1
    # correspondence between Gid and Number, even in different versions of same
    # stop.
    STOP_QUERY = """
        SELECT DISTINCT
            CONVERT(CHAR(16), Gid) AS JourneyPatternPointGid,
            CONVERT(CHAR(7), Number) AS JoreStopId
        FROM
            JourneyPatternPoint
        WHERE
            Number < 1999000
            OR Number > 1999999
    """
    JOURNEY_QUERY = """
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
    UTC_OFFSET_QUERY = """
        SELECT
            CONVERT(CHAR(16), Id) AS DatedVehicleJourneyId,
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
        self._event_poll_sleep_in_seconds = util.convert_duration_to_seconds(
            config['event_poll_sleep'])

        # Get Jore information from PubTrans IDs using Mappers.
        self._stop_mapper = mapper.Mapper(self._update_stops, _parse_stops)
        self._journey_mapper = mapper.Mapper(self._update_journeys,
                                             _parse_journeys)
        self._utc_offset_mapper = mapper.Mapper(self._update_utc_offsets,
                                                _parse_utc_offsets)

        # Parameters for avoiding forwarding predictions too similar to
        # previous ones.
        self._prediction_change_threshold_in_seconds = config[
            'prediction_change_threshold_in_seconds']
        self._prediction_cache_size = config['prediction_cache_size']
        self._event_cache_size = config['event_cache_size']

        self._prediction_mqtt_topic_mid = config['prediction_mqtt_topic_mid']
        self._event_mqtt_topic_mid = config['event_mqtt_topic_mid']

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
        LOG.info('Updating stop mapping.')
        query = PredictionPoller.STOP_QUERY
        LOG.debug('Querying stops from DOI:%s', query)
        result = await self._connect_and_query(self._doi_connect, query)
        LOG.debug('Got %d stops.', len(result))
        return result

    async def _update_journeys(self):
        LOG.info('Updating journey mapping.')
        now = datetime.datetime.utcnow()
        past_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_LEAST_DAYS_BACK_SHIFT)
        future_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_MOST_DAYS_FORWARD_SHIFT)
        query = PredictionPoller.JOURNEY_QUERY.format(
            past_utc=past_utc, future_utc=future_utc)
        LOG.debug('Querying journeys from DOI:%s', query)
        result = await self._connect_and_query(self._doi_connect, query)
        LOG.debug('Got %d journeys.', len(result))
        return result

    async def _update_utc_offsets(self):
        LOG.info('Updating UTC offset mapping.')
        now = datetime.datetime.utcnow()
        past_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_LEAST_DAYS_BACK_SHIFT)
        future_utc = _timestamp_day_shift(
            now, PredictionPoller._AT_MOST_DAYS_FORWARD_SHIFT)
        query = PredictionPoller.UTC_OFFSET_QUERY.format(
            past_utc=past_utc, future_utc=future_utc)
        LOG.debug('Querying UTC offsets from ROI:%s', query)
        result = await self._connect_and_query(self._roi_connect, query)
        LOG.debug('Got %d UTC offsets.', len(result))
        return result

    async def _update_mappers(self):
        tasks = [
            self._stop_mapper.update(),
            self._journey_mapper.update(),
            self._utc_offset_mapper.update(),
        ]
        done, pending = await self._async_helper.wait(tasks)
        if len(pending) > 0 or len(done) < len(tasks):
            LOG.error('At least one of the mapping updates failed. In '
                      'pending: %s', str(pending))
        return any((future.result() for future in done))

    def _get_matches(self, row):
        jpp = row[2]
        stop = self._stop_mapper.get(jpp)
        if stop is None:
            LOG.debug('This JourneyPatternPointGid was not found from '
                      'collected stop information: %s. Row was: %s', jpp,
                      str(row))
        dvj = row[1]
        journey_info = self._journey_mapper.get(dvj)
        if journey_info is None:
            LOG.debug('This DatedVehicleJourneyId was not found from '
                      'collected journey information: %s. Row was: %s', dvj,
                      str(row))
        utc_offset = self._utc_offset_mapper.get(dvj)
        if utc_offset is None:
            LOG.debug('This DatedVehicleJourneyId was not found from '
                      'collected UTC offset information: %s. Row was: %s', dvj,
                      str(row))
        if stop is None or journey_info is None or utc_offset is None:
            return None
        return {
            'stop': stop,
            'journey_info': journey_info,
            'utc_offset': utc_offset,
        }

    async def _get_all_matches(self, rows):
        is_every_row_matched = False
        matches = []
        while not is_every_row_matched:
            matches = [self._get_matches(row) for row in rows]
            if await self._update_mappers():
                LOG.debug('At least one Jore mapper was updated so try '
                          'matching again.')
            else:
                is_every_row_matched = True
        return matches

    async def _keep_polling_predictions(self):
        prediction_filter = PredictionFilter()
        filter_similar_predictions = _create_filter(
            cache_size=self._prediction_cache_size,
            extract=_extract_arrival_id_and_prediction,
            check_for_change=functools.partial(
                _check_prediction_for_change,
                self._prediction_change_threshold_in_seconds))
        modified_utc_dt = (datetime.datetime.utcnow() - datetime.timedelta(
            seconds=self._prediction_poll_sleep_in_seconds))
        while True:
            modified_utc = _format_datetime_for_sql(modified_utc_dt)
            query = PredictionPoller.PREDICTION_QUERY.format(
                modified_utc=modified_utc)
            LOG.debug('Prediction polling starting to wait for the MQTT '
                      'connection.')
            await self._is_mqtt_connected.wait()
            LOG.debug('Querying predictions modified at or after %s from ROI.',
                      modified_utc_dt.isoformat())
            result = await self._connect_and_query(self._roi_connect, query)
            message_timestamp = _create_timestamp()
            old_len = len(result)
            result = prediction_filter.filter(result)
            result = filter_similar_predictions(result)
            new_len = len(result)
            LOG.debug('Got %s predictions of which %s were new.', old_len,
                      new_len)
            if result:
                matches = await self._get_all_matches(result)
                predictions_by_stop = _arrange_predictions_by_stop(result,
                                                                   matches)
                for stop, predictions in predictions_by_stop.items():
                    topic_suffix = self._prediction_mqtt_topic_mid + stop
                    message = {
                        'messageTimestamp': message_timestamp,
                        'predictions': predictions,
                    }
                    await self._queue.put((topic_suffix, json.dumps(message)))
                prediction_filter.update(result)
                modified_utc_dt = prediction_filter.get_latest_modification_datetime()
            await self._async_helper.sleep(
                self._prediction_poll_sleep_in_seconds)

    async def _keep_polling_events(self):
        filter_repeated_events = _create_filter(
            cache_size=self._event_cache_size,
            extract=_extract_departure_id_and_event,
            check_for_change=operator.ne)
        modified_utc_dt = (datetime.datetime.utcnow() - datetime.timedelta(
            seconds=self._event_poll_sleep_in_seconds))
        while True:
            modified_utc = _format_datetime_for_sql(modified_utc_dt)
            query = PredictionPoller.EVENT_QUERY.format(
                modified_utc=modified_utc)
            LOG.debug('Event polling starting to wait for the MQTT '
                      'connection.')
            await self._is_mqtt_connected.wait()
            LOG.debug('Querying events modified at or after %s from ROI.',
                      modified_utc_dt.isoformat())
            result = await self._connect_and_query(self._roi_connect, query)
            message_timestamp = _create_timestamp()
            old_len = len(result)
            result = filter_repeated_events(result)
            new_len = len(result)
            LOG.debug('Got %s events of which %s were new.', old_len, new_len)
            if result:
                matches = await self._get_all_matches(result)
                events_by_stop = _arrange_events_by_stop(result, matches)
                for stop, events in events_by_stop.items():
                    topic_suffix = self._event_mqtt_topic_mid + stop
                    message = {
                        'messageTimestamp': message_timestamp,
                        'events': events,
                    }
                    await self._queue.put((topic_suffix, json.dumps(message)))
                modified_utc_dt = max(row[5] for row in result)
            await self._async_helper.sleep(self._event_poll_sleep_in_seconds)

    async def run(self):
        """Run the PredictionPoller."""
        LOG.debug('Starting to poll events and predictions.')
        futures = [
            self._async_helper.ensure_future(self._keep_polling_predictions()),
            self._async_helper.ensure_future(self._keep_polling_events()),
        ]
        await self._async_helper.wait_for_first(futures)
        LOG.error('Prediction polling ended unexpectedly.')
