# -*- coding: utf-8 -*-
"""Poll the PubTrans SQL database."""

import collections
import datetime
import functools
import json
import logging

import cachetools

from roisto import util
from roisto import sqlconnector
from roisto.match import journey
from roisto.match import stop
from roisto.match import utcoffset

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

##############
# Filtering. #
##############


def _create_filter(cache_size, extract_cache_key_value, is_included):
    cache = cachetools.LRUCache(maxsize=cache_size)

    def is_included_and_cached(matched):
        """Check whether a value should be included and thus also cached."""
        key, current = extract_cache_key_value(matched)
        cached = cache.get(key, None)
        is_kept = is_included(current, cached)
        if is_kept:
            cache[key] = current
        return is_kept

    def filter_(matches):
        """Keep only interesting matches."""
        kept = list(filter(is_included_and_cached, matches))
        LOG.debug('%s rows remain after filtering with %s.',
                  str(len(kept)), is_included.__name__)
        return kept

    return filter_


def _check_event_for_inclusion(current, cached):
    """Check that the event is interesting and has changed."""
    is_kept = False
    # FIXME: Map integers to strings earlier and compare to strings here.
    if current in [9, 10, 11]:
        if cached is None:
            is_kept = True
        else:
            is_kept = current != cached
    return is_kept


def _extract_departure_id_and_event(matched):
    return matched['source']['DepartureId'], matched['source']['State']


def _check_prediction_for_inclusion(pre_journey_threshold_in_s,
                                    change_threshold_in_s, current, cached):
    """Rule out too early predictions and predictions with little change.

    Check that a prediction is not sent too early (VPC bug) and that the
    prediction has changed enough since last time it changed enough.
    """
    is_kept = False
    is_given_early = (
        current['StartUTCDateTime'] - current['LastModifiedUTCDateTime']
    ).total_seconds() > pre_journey_threshold_in_s
    is_predicted_early = current['TargetDateTime'] < current[
        'TimetabledEarliestDateTime']
    if not (is_given_early and is_predicted_early):
        if cached is None:
            is_kept = True
        else:
            is_kept = abs((current['TargetDateTime'] - cached['TargetDateTime']
                           ).total_seconds()) >= change_threshold_in_s
    return is_kept


def _extract_departure_id_and_prediction(matched):
    source = matched['source']
    d = {
        k: source[k]
        for k in [
            'TimetabledEarliestDateTime',
            'TargetDateTime',
            'LastModifiedUTCDateTime',
        ]
    }
    d['StartUTCDateTime'] = (
        matched['journey']['LocalizedStartTime'] - datetime.timedelta(
            minutes=matched['utc_offset']['UTCOffsetMinutes']))
    return source['DepartureId'], d


################
# Serializing. #
################


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


def _create_arranger(arrange):
    def arrange_by_key(matches):
        by_key = collections.defaultdict(list)
        for matched in matches:
            key, value = arrange(matched)
            by_key[key].append(value)
        return dict(by_key)

    return arrange_by_key


def _arrange_prediction(matched):
    source = matched['source']
    stop = matched['stop']['JoreStopId']
    journey = matched['journey']
    utc_offset = matched['utc_offset']['UTCOffsetMinutes']

    start_naive = journey['LocalizedStartTime']
    scheduled_naive = source['TimetabledEarliestDateTime']
    predicted_naive = source['TargetDateTime']
    start_time = _combine_into_timestamp(start_naive, utc_offset)
    scheduled_time = _combine_into_timestamp(scheduled_naive, utc_offset)
    predicted_time = _combine_into_timestamp(predicted_naive, utc_offset)
    prediction = {
        'joreStopId': stop,
        'joreLineId': journey['JoreLineId'],
        'joreLineDirection': journey['JoreDirection'],
        'journeyStartTime': start_time,
        'scheduledArrivalTime': scheduled_time,
        'predictedArrivalTime': predicted_time,
    }
    return stop, prediction


def _arrange_event(matched):
    source = matched['source']
    stop = matched['stop']['JoreStopId']
    journey = matched['journey']
    utc_offset = matched['utc_offset']['UTCOffsetMinutes']

    start_naive = journey['LocalizedStartTime']
    scheduled_naive = source['TimetabledEarliestDateTime']
    event = DEPARTURE_STATES[source['State']]
    start_time = _combine_into_timestamp(start_naive, utc_offset)
    scheduled_time = _combine_into_timestamp(scheduled_naive, utc_offset)
    prediction = {
        'joreStopId': stop,
        'joreLineId': journey['JoreLineId'],
        'joreLineDirection': journey['JoreDirection'],
        'journeyStartTime': start_time,
        'scheduledArrivalTime': scheduled_time,
        'event': event,
    }
    return stop, prediction


def _create_event_serializer(mqtt_topic_mid):
    arrange_seq = _create_arranger(_arrange_event)

    def serialize(matches, message_timestamp):
        serialized = []
        events_by_stop = arrange_seq(matches)
        for stop_, events in events_by_stop.items():
            topic_suffix = mqtt_topic_mid + stop_
            message = {
                'messageTimestamp': message_timestamp,
                'events': events,
            }
            serialized.append((topic_suffix, json.dumps(message)))
        return serialized

    return serialize


def _create_prediction_serializer(mqtt_topic_mid):
    arrange_seq = _create_arranger(_arrange_prediction)

    def serialize(matches, message_timestamp):
        serialized = []
        predictions_by_stop = arrange_seq(matches)
        for stop_, predictions in predictions_by_stop.items():
            topic_suffix = mqtt_topic_mid + stop_
            message = {
                'messageTimestamp': message_timestamp,
                'predictions': predictions,
            }
            serialized.append((topic_suffix, json.dumps(message)))
        return serialized

    return serialize


#######################################
# Filtering, serializing, forwarding. #
#######################################


def _create_processor(filter_, serialize, queue):
    async def process(matched, message_timestamp):
        filtered = filter_(matched)
        serialized = serialize(filtered, message_timestamp)
        for topic_suffix, message in serialized:
            await queue.put((topic_suffix, message))

    return process


def _create_timestamp():
    return _combine_into_timestamp(datetime.datetime.utcnow(), 0)


def _format_datetime_for_sql(dt):
    return dt.strftime('%Y%m%d %H:%M:%S.') + dt.strftime('%f')[:3]


class Poller:
    """Poll predictions and events, match to Jore and forward to MONO."""

    # Cut off via points.
    #
    # At 2016-10-26T12:32Z it holds for every row of JourneyPatternPoint in
    # ptDOI that:
    # Gid % 10000000 = Number
    # So cut off via points that way.
    #
    # FIXME: Use rda* tables when they are ready. Meanwhile, use numeric
    #        values.
    # FIXME: To speed things up, do not join with DatedVehicleJourney. We do
    # get extra events then, though.
    POLLING_QUERY = """
        SELECT
            CONVERT(CHAR(16), D.Id) AS DepartureId,
            CONVERT(CHAR(16), D.IsOnDatedVehicleJourneyId) AS DatedVehicleJourneyId,
            CONVERT(CHAR(16), D.IsTargetedAtJourneyPatternPointGid
            ) AS JourneyPatternPointGid,
            D.TimetabledEarliestDateTime,
            D.TargetDateTime,
            D.State,
            D.LastModifiedUTCDateTime
        FROM
            Departure AS D
        WHERE
            D.LastModifiedUTCDateTime >= '{modified_utc}'
            AND D.LastModifiedUTCDateTime IS NOT NULL
            AND (
                D.IsTargetedAtJourneyPatternPointGid % 10000000 < 1999000
                OR D.IsTargetedAtJourneyPatternPointGid % 10000000 > 1999999
            )
    """

    def __init__(self, config, async_helper, queue, is_mqtt_connected):
        self._async_helper = async_helper
        self._queue = queue
        self._is_mqtt_connected = is_mqtt_connected

        # Connecting functions.
        self._sql_connector = sqlconnector.SQLConnector(config['sql'],
                                                        self._async_helper)
        # Get Jore information from PubTrans IDs using Mappers.
        self._stop_mapper = stop.create_stop_mapper(self._sql_connector)
        self._journey_mapper = journey.create_journey_mapper(
            self._sql_connector)
        self._utc_offset_mapper = utcoffset.create_utc_offset_mapper(
            self._sql_connector)

        self._poll_interval_in_seconds = util.convert_duration_to_seconds(
            config['poll_interval'])

        self._pre_journey_prediction_threshold_in_seconds = config[
            'pre_journey_prediction_threshold_in_seconds']
        self._prediction_change_threshold_in_seconds = config[
            'prediction_change_threshold_in_seconds']
        self._prediction_cache_size = config['prediction_cache_size']
        self._event_cache_size = config['event_cache_size']

        self._prediction_mqtt_topic_mid = config['prediction_mqtt_topic_mid']
        self._event_mqtt_topic_mid = config['event_mqtt_topic_mid']

    def _get_matches(self, row):
        jpp = row['JourneyPatternPointGid']
        stop = self._stop_mapper.get(jpp)
        if stop is None:
            LOG.debug('This JourneyPatternPointGid was not found from '
                      'collected stop information: %s. Row was: %s', jpp,
                      str(row))
        dvj = row['DatedVehicleJourneyId']
        journey = self._journey_mapper.get(dvj)
        if journey is None:
            LOG.debug('This DatedVehicleJourneyId was not found from '
                      'collected journey information: %s. Row was: %s', dvj,
                      str(row))
        utc_offset = self._utc_offset_mapper.get(dvj)
        if utc_offset is None:
            LOG.debug('This DatedVehicleJourneyId was not found from '
                      'collected UTC offset information: %s. Row was: %s', dvj,
                      str(row))
        if stop is None or journey is None or utc_offset is None:
            return None
        return {
            'source': row,
            'stop': stop,
            'journey': journey,
            'utc_offset': utc_offset,
        }

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
        matches = [x for x in matches if x is not None]
        LOG.debug('%s rows remain after matching to Jore information.',
                  str(len(matches)))
        return matches

    async def _poll(self, processors, modified_utc_dt):
        modified_utc = _format_datetime_for_sql(modified_utc_dt)
        query = Poller.POLLING_QUERY.format(modified_utc=modified_utc)
        LOG.debug('Polling starting to wait for the MQTT connection.')
        await self._is_mqtt_connected.wait()
        LOG.debug('Querying for modifications at or after %s from ptROI.',
                  modified_utc_dt.isoformat())
        rows = await self._sql_connector.query_from_roi(query)
        if rows:
            message_timestamp = _create_timestamp()
            LOG.debug('Polling got %s rows.', str(len(rows)))
            matched = await self._get_all_matches(rows)
            tasks = [
                self._async_helper.ensure_future(
                    process(matched, message_timestamp))
                for process in processors
            ]
            await self._async_helper.wait_forever(tasks)
            modified_utc_dt = max(row['LastModifiedUTCDateTime']
                                  for row in rows)
        return modified_utc_dt

    async def _keep_polling(self):
        event_processor = _create_processor(
            filter_=_create_filter(
                cache_size=self._event_cache_size,
                extract_cache_key_value=_extract_departure_id_and_event,
                is_included=_check_event_for_inclusion),
            serialize=_create_event_serializer(self._event_mqtt_topic_mid),
            queue=self._queue)
        prediction_processor = _create_processor(
            filter_=_create_filter(
                cache_size=self._prediction_cache_size,
                extract_cache_key_value=_extract_departure_id_and_prediction,
                is_included=functools.partial(
                    _check_prediction_for_inclusion,
                    self._pre_journey_prediction_threshold_in_seconds,
                    self._prediction_change_threshold_in_seconds)),
            serialize=_create_prediction_serializer(
                self._prediction_mqtt_topic_mid),
            queue=self._queue)
        processors = [
            event_processor,
            prediction_processor,
        ]
        modified_utc_dt = (datetime.datetime.utcnow() - datetime.timedelta(
            seconds=self._poll_interval_in_seconds))
        while True:
            poll_fut = self._async_helper.ensure_future(
                self._poll(processors, modified_utc_dt))
            futures = [
                poll_fut,
                self._async_helper.ensure_future(
                    self._async_helper.sleep(self._poll_interval_in_seconds)),
            ]
            await self._async_helper.wait_forever(futures)
            modified_utc_dt = poll_fut.result()

    async def run(self):
        """Run the Poller."""
        LOG.debug('Starting to poll events and predictions.')
        await self._async_helper.ensure_future(self._keep_polling())
        LOG.error('Prediction polling ended unexpectedly.')
