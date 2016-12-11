# -*- coding: utf-8 -*-
"""Test prediction filtering."""

import datetime
import functools

import roisto.predictionpoller


def test_check_prediction_for_change():
    current = datetime.datetime(2016, 11, 24, 11, 51)
    old = datetime.datetime(2016, 11, 24, 11, 50, 32, 227000)
    assert roisto.predictionpoller._check_prediction_for_change(10, current,
                                                                old)
    assert not roisto.predictionpoller._check_prediction_for_change(
        30, current, old)


def test_similar_prediction_filter():
    first_rows = [
        (
            '7990500820220289',
            '7990500820220297',
            '20161124:9015301028009377',
            '9025301004730501',
            datetime.datetime(2016, 11, 24, 11, 51),
            datetime.datetime(2016, 11, 24, 11, 50, 32, 227000),
            datetime.datetime(2016, 11, 24, 9, 33, 19, 440000), ),
        (
            '7990501258711017',
            '7990501258711037',
            '20161124:9015301012300158',
            '9025301001491129',
            datetime.datetime(2016, 11, 24, 11, 33),
            datetime.datetime(2016, 11, 24, 11, 33, 25, 860000),
            datetime.datetime(2016, 11, 24, 9, 33, 21, 480000), ),
    ]
    second_rows = [
        (
            '7990500820220289',
            '7990500820220297',
            '20161124:9015301028009377',
            '9025301004730501',
            datetime.datetime(2016, 11, 24, 11, 51),
            datetime.datetime(2016, 11, 24, 11, 50, 34, 227000),
            datetime.datetime(2016, 11, 24, 9, 33, 19, 440000), ),
        (
            '7990501258711017',
            '7990501258711037',
            '20161124:9015301012300158',
            '9025301001491129',
            datetime.datetime(2016, 11, 24, 11, 33),
            datetime.datetime(2016, 11, 24, 11, 33, 12, 860000),
            datetime.datetime(2016, 11, 24, 9, 33, 21, 480000), ),
    ]
    filter_similar_predictions = roisto.predictionpoller._create_filter(
        cache_size=10,
        extract=roisto.predictionpoller._extract_arrival_id_and_prediction,
        check_for_change=functools.partial(
            roisto.predictionpoller._check_prediction_for_change, 5))

    first_result = filter_similar_predictions(first_rows)
    assert first_result == first_rows
    second_result = filter_similar_predictions(second_rows)
    assert second_result == second_rows[1:]
