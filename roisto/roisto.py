#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main module."""

import asyncio
import logging
import logging.config

from roisto import cmdline
from roisto import mqttpublisher
from roisto import predictionpoller
from roisto import util


def main():
    """Main function."""
    args = cmdline.parse_cmdline()
    config = args.config

    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger(__name__)
    logger.info('roisto started.')

    loop = asyncio.get_event_loop()
    async_helper = util.AsyncHelper(loop, executor=None)

    queue = asyncio.Queue()
    is_mqtt_connected = asyncio.Event(loop=loop)

    mqtt_publisher = mqttpublisher.MQTTPublisher(config['mqtt'], async_helper,
                                                 queue, is_mqtt_connected)

    prediction_poller = predictionpoller.PredictionPoller(
        config['pubtrans'], async_helper, queue, is_mqtt_connected)

    tasks = [
        mqtt_publisher.run(),
        prediction_poller.run(),
    ]
    loop.run_until_complete(async_helper.wait_for_first(tasks))
    logger.error('Either mqtt_publisher or prediction_poller completed.')
    loop.close()


if __name__ == '__main__':
    main()
