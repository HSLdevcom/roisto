# -*- coding: utf-8 -*-
"""Common, shared utilities for mappers."""

import datetime
import logging

LOG = logging.getLogger(__name__)

_WINDOW_START_SHIFT_IN_DAYS = -1
_WINDOW_END_SHIFT_IN_DAYS = 2


def parse_mapper_query_results(key_name, rows):
    result = {}
    for d in rows:
        key = d.pop(key_name, None)
        if key is None:
            LOG.warning('%s not found from this row: %s', key_name, str(d))
        else:
            result[key] = d
    return result


def _timestamp_day_shift(now, days):
    then = now + datetime.timedelta(days=days)
    return then.strftime('%Y-%m-%d')


def get_window():
    now = datetime.datetime.utcnow()
    past_utc = _timestamp_day_shift(now, _WINDOW_START_SHIFT_IN_DAYS)
    future_utc = _timestamp_day_shift(now, _WINDOW_END_SHIFT_IN_DAYS)
    return past_utc, future_utc
