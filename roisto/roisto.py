#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main module."""

import asyncio
import concurrent.futures
import logging
import logging.config

from roisto import cmdline
from roisto import mqttpublisher
from roisto import poller


def main():
    """Main function."""
    args = cmdline.parse_cmdline()
    config = args.config

    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger(__name__)
    logger.info('roisto started.')

    loop = asyncio.get_event_loop()

    queue = asyncio.Queue()
    is_mqtt_connected = asyncio.Event(loop=loop)

    mqtt_publisher = mqttpublisher.MQTTPublisher(config['mqtt'], loop, queue,
                                                 is_mqtt_connected)
    poller_ = poller.Poller(config['pubtrans'], loop, queue, is_mqtt_connected)

    tasks = [
        mqtt_publisher.run(),
        poller_.run(),
    ]
    loop.run_until_complete(
        asyncio.wait(
            tasks, loop=loop, return_when=concurrent.futures.FIRST_COMPLETED))

    logger.error('Either mqtt_publisher or poller_ completed.')
    loop.close()


if __name__ == '__main__':
    main()
