# -*- coding: utf-8 -*-
"""Poll the PubTrans SQL database."""

import datetime
import functools
import json
import logging

import isodate
import pymssql

from roisto import util

LOG = logging.getLogger(__name__)


def _minutes_to_hours_string(minutes):
    MINUTES_IN_HOUR = 60
    sign = '+'
    if minutes < 0:
        sign = '-'
    hours, minutes_left = divmod(abs(minutes), MINUTES_IN_HOUR)
    return '{sign}{hours:02d}:{minutes:02d}'.format(
        sign=sign, hours=hours, minutes=minutes_left)


def _combine_into_timestamp(naive_datetime, utc_offset_minutes):
    return (naive_datetime.isoformat() +
            _minutes_to_hours_string(utc_offset_minutes))


def _connect_and_query(connect, query):
    """Connect and query once."""
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()


def _timestamp_day_shift(now, days):
    then = now + datetime.timedelta(days=days)
    return then.strftime('%Y-%m-%d')


class PredictionPoller:
    """Polls for predictions and forwards MONO messages.

    PredictionPoller also polls for information that allows mapping PubTrans
    IDs to Jore IDs.
    """

    _AT_LEAST_DAYS_BACK_SHIFT = -1
    _AT_MOST_DAYS_FORWARD_SHIFT = 2

    def __init__(self, config, async_helper, queue, is_mqtt_connected):
        self._async_helper = async_helper
        self._queue = queue
        self._is_mqtt_connected = is_mqtt_connected

        # Connecting functions.
        self._connect = functools.partial(
            pymssql.connect,
            server=config['host'],
            user=config['user'],
            password=config['password'],
            port=config['port'])
        self._doi_connect = functools.partial(
            self._connect, database=config['doi'])
        self._roi_connect = functools.partial(
            self._connect, database=config['roi'])

        # Dictionaries for matching PubTrans IDs to Jore IDs.
        self._stops = {}
        self._lines = {}

        # Accounting for the timezone.
        self._utc_offsets = {}

        # Query intervals.
        self._stop_query_interval_in_seconds = util.convert_duration_to_seconds(
            config['stop_query_interval'])
        self._line_query_interval_in_seconds = util.convert_duration_to_seconds(
            config['line_query_interval'])

    # FIXME: Use or remove.
    ## Example usage:
    ## await self._process_and_sleep(roi_connect, query_string, handle_utc_offsets, self._utc_offset_query_interval_in_seconds)
    #async def _process_and_sleep(self, connect, query_string, handle_result, sleep_in_seconds):
    #    try:
    #        result = await self._async_helper.run_in_executor(_connect_and_query, connect, query_string)
    #        handle_result(result)
    #    except pymssql.Error as ex:
    #        LOG.warning('SQL error: ' + str(ex))
    #    except pymssql.Warning as ex:
    #        LOG.warning('SQL warning: ' + str(ex))
    #    return await self._async_helper.sleep(sleep_in_seconds)

    async def _keep_polling_stops(self):
        STOP_QUERY = """
            SELECT DISTINCT
                CONVERT(CHAR(16), Gid) AS JourneyPatternPointGid,
                CONVERT(CHAR(7), Number) AS JoreStopId
            FROM
                JourneyPatternPoint
        """
        while True:
            try:
                result = await self._async_helper.run_in_executor(
                    _connect_and_query, self._doi_connect, STOP_QUERY)
                self._stops = dict(result)
            except pymssql.Error as ex:
                LOG.warning('SQL error: ' + str(ex))
            except pymssql.Warning as ex:
                LOG.warning('SQL warning: ' + str(ex))
            await self._async_helper.sleep(
                self._stop_query_interval_in_seconds)

    async def _keep_polling_lines(self):
        LINE_QUERY = """
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
        """
        while True:
            now = datetime.datetime.utcnow()
            past_utc = _timestamp_day_shift(
                now, PredictionPoller._AT_LEAST_DAYS_BACK_SHIFT)
            future_utc = _timestamp_day_shift(
                now, PredictionPoller._AT_MOST_DAYS_FORWARD_SHIFT)
            query = LINE_QUERY.format(past_utc=past_utc, future_utc=future_utc)
            try:
                result = await self._async_helper.run_in_executor(
                    _connect_and_query, self._doi_connect, query)
                rearranged = {}
                for row in result:
                    rearranged[row[0]] = {
                        'JoreLineId': row[1],
                        'JoreDirection': row[2],
                        'LocalizedStartTime': row[3],
                    }
                self._lines = rearranged
            except pymssql.Error as ex:
                LOG.warning('SQL error: ' + str(ex))
            except pymssql.Warning as ex:
                LOG.warning('SQL warning: ' + str(ex))
            await self._async_helper.sleep(
                self._line_query_interval_in_seconds)

    async def _keep_polling_utc_offsets(self):
        UTC_OFFSET_QUERY = """
            SELECT
                Id AS DatedVehicleJourneyId,
                UTCOffsetMinutes
            FROM
                DatedVehicleJourney
            WHERE
                DVJ.OperatingDayDate >= '${past_utc}'
                AND DVJ.OperatingDayDate < '${future_utc}'
        """
        while True:
            now = datetime.datetime.utcnow()
            past_utc = _timestamp_day_shift(
                now, PredictionPoller._AT_LEAST_DAYS_BACK_SHIFT)
            future_utc = _timestamp_day_shift(
                now, PredictionPoller._AT_MOST_DAYS_FORWARD_SHIFT)
            query = UTC_OFFSET_QUERY.format(
                past_utc=past_utc, future_utc=future_utc)
            try:
                result = await self._async_helper.run_in_executor(
                    _connect_and_query, self._roi_connect, query)
                self._utc_offsets = dict(result)
            except pymssql.Error as ex:
                LOG.warning('SQL error: ' + str(ex))
            except pymssql.Warning as ex:
                LOG.warning('SQL warning: ' + str(ex))
            await self._async_helper.sleep(
                self._utc_offset_query_interval_in_seconds)

    def _gather_predictions_per_stop(self, result):
        predictions_by_stop = {}
        # FIXME: Check for initialization only once in a cleaner way.
        if (self._stops is not None and self._lines is not None and
                self._utc_offsets is not None):
            for row in result:
                dvj = row[1]
                line_info = self._lines.get(dvj, None)
                if line_info is None:
                    LOG.warning('This DatedVehicleJourneyId was not found '
                                'from collected line information: ' + dvj)
                    continue
                utc_offset = self._utc_offsets.get(dvj, None)
                if utc_offset is None:
                    LOG.warning('This DatedVehicleJourneyId was not found '
                                'from collected UTC offset information: ' +
                                dvj)
                    continue
                jpp = row[2]
                stop = self._stops.get(jpp, None)
                if stop is None:
                    LOG.warning('This JourneyPatternPointGid was not found '
                                'from collected stop information: ' + jpp)
                    continue
                # FIXME: Finish this
                # Use strings from sql server, add hours in the end + 03:00
                start_naive = line_info['LocalizedStartTime']
                scheduled_naive = row[3]
                predicted_naive = row[4]
                start_time = _combine_into_timestamp(start_naive, utc_offset)
                scheduled_time = _combine_into_timestamp(scheduled_naive,
                                                         utc_offset)
                predicted_time = _combine_into_timestamp(predicted_naive,
                                                         utc_offset)
                prediction = {
                    'joreStopId': stop,
                    'joreLineId': line_info['JoreLineId'],
                    'joreLineDirection': line_info['JoreDirection'],
                    'journeyStartTime': start_time,
                    'scheduledArrivalTime': scheduled_time,
                    'predictedArrivalTime': predicted_time,
                }
                predictions_list = predictions_by_stop.get(stop, [])
                predictions_list.append(prediction)
                predictions_by_stop[stop] = predictions_list
        else:
            LOG.info('Throwing away predictions as matching information is '
                     'not yet available.')
        return predictions_by_stop

    async def _keep_polling_predictions(self):
        PREDICTION_QUERY = """
            SELECT
                CONVERT(CHAR(16), Arrival.Id) AS ArrivalId,
                CONVERT(CHAR(16), DatedVehicleJourney.Id)
                AS DatedVehicleJourneyId,
                CONVERT(CHAR(16), Arrival.IsTargetedAtJourneyPatternPointGid)
                AS JourneyPatternPointGid,
                Arrival.TimetabledLatestDateTime,
                Arrival.EstimatedDateTime,
                Arrival.LastModifiedUTCDateTime
            FROM
                Arrival,
                DatedVehicleJourney
            WHERE
                Arrival.IsOnDatedVehicleJourneyId = DatedVehicleJourney.Id
                AND Arrival.LastModifiedUTCDateTime >= '{modified_utc}'
                AND Arrival.EstimatedDateTime IS NOT NULL
                AND Arrival.LastModifiedUTCDateTime IS NOT NULL
        """
        now = datetime.datetime.utcnow()
        modified_utc_dt = (now - datetime.timedelta(
            seconds=self._prediction_query_interval_in_seconds))
        while True:
            modified_utc = (modified_utc_dt.strftime('%Y%m%d %H:%M:%S.') +
                            modified_utc_dt.strftime('%f')[:3])
            query = PREDICTION_QUERY.format(modified_utc=modified_utc)
            await self._async_helper.wait_for_event(self._is_mqtt_connected)
            try:
                result = await self._async_helper.run_in_executor(
                    _connect_and_query, self._roi_connect, query)
                message_timestamp = isodate.datetime_isoformat(
                    datetime.datetime.utcnow())
                # We will get the latest predictions again next time but it is
                # more important not to miss any predictions than to not repeat
                # predictions.
                #
                # Note that at least on 2016-09-13 the Microsoft SQL Server
                # datetime data type has less than a millisecond precision due
                # to rounding.
                modified_utc_dt = max(row[5] for row in result)
                predictions_by_stop = self._gather_predictions_per_stop(result)
                for stop_id, predictions in predictions_by_stop.items():
                    topic_suffix = stop_id
                    message = {
                        'messageTimestamp': message_timestamp,
                        'predictions': predictions,
                    }
                    await self._queue.put((topic_suffix, json.dumps(message)))
            except pymssql.Error as ex:
                LOG.warning('SQL error: ' + str(ex))
            except pymssql.Warning as ex:
                LOG.warning('SQL warning: ' + str(ex))
            await self._async_helper.sleep(
                self._prediction_query_interval_in_seconds)

    async def run(self):
        """Run the PredictionPoller."""
        LOG.debug('PredictionPoller runs.')
        tasks = [
            self._keep_polling_stops(),
            self._keep_polling_lines(),
            self._keep_polling_utc_offsets(),
            self._keep_polling_predictions(),
        ]
        await self._async_helper.wait_for_first(tasks)
        LOG.error('An SQL polling coroutine got completed.')
