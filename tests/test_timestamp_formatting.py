# -*- coding: utf-8 -*-
"""Test timestamp formatting."""

from roisto import predictionpoller

def test_minutes_to_hours_string():
    assert predictionpoller._minutes_to_hours_string(-3) == '-00:03'
    assert predictionpoller._minutes_to_hours_string(180) == '+03:00'
    assert predictionpoller._minutes_to_hours_string(-64) == '-01:04'
    assert predictionpoller._minutes_to_hours_string(-640) == '-10:40'
    assert predictionpoller._minutes_to_hours_string(640) == '+10:40'
