# -*- coding: utf-8 -*-
"""Test prediction filtering."""

import datetime

import roisto.predictionpoller


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
    filter_similar_predictions = roisto.predictionpoller._create_similar_prediction_filter(
        5, 10)

    first_result = filter_similar_predictions(first_rows)
    assert first_result == first_rows
    second_result = filter_similar_predictions(second_rows)
    assert second_result == second_rows[1:]
