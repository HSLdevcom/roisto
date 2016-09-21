# -*- coding: utf-8 -*-
"""Publish MQTT messages."""

import logging

import paho.mqtt.client as mqtt

LOG = logging.getLogger(__name__)


class MQTTPublisher:
    """Publish messages from a given queue using MQTT."""

    _LOG_MATCH = {
        mqtt.MQTT_LOG_DEBUG: logging.DEBUG,
        mqtt.MQTT_LOG_INFO: logging.INFO,
        mqtt.MQTT_LOG_NOTICE: logging.INFO,
        mqtt.MQTT_LOG_WARNING: logging.WARNING,
        mqtt.MQTT_LOG_ERR: logging.ERROR,
    }

    def __init__(self, config, async_helper, queue, is_mqtt_connected):
        self._async_helper = async_helper
        self._queue = queue
        self._is_mqtt_connected = is_mqtt_connected

        self._host = config['host']
        self._port = config['port']
        self._topic_prefix = config['topic_prefix']
        self._qos = config['qos']

        self._client = self._create_client(config)

    def _create_client(self, config):
        client = mqtt.Client(
            client_id=config['client_id'], transport=config['transport'])
        client.on_connect = self._cb_on_connect
        client.on_disconnect = self._cb_on_disconnect
        client.on_log = self._cb_on_log
        tls_path = config.get('ca_certs_path', None)
        if tls_path is not None:
            client.tls_set(tls_path)
        return client

    def _cb_on_connect(self, mqtt_client, userdata, flags, rc):
        if rc == 0:
            LOG.info('MQTT connection attempt succeeded.')
            self._is_mqtt_connected.set()
        else:
            LOG.warning('MQTT connection attempt failed: %s',
                        mqtt.connack_string(rc))
            self._is_mqtt_connected.clear()

    def _cb_on_disconnect(self, mqtt_client, userdata, rc):
        if rc == 0:
            LOG.info('Disconnection succeeded.')
        else:
            LOG.warning('Lost MQTT connection: %s', mqtt.error_string(rc))
        self._is_mqtt_connected.clear()

    def _cb_on_log(self, mqtt_client, userdata, level, buf):
        log_level = MQTTPublisher._LOG_MATCH[level]
        LOG.log(log_level, 'paho-mqtt: %s', buf)

    async def _keep_publishing(self):
        while True:
            topic, payload = await self._queue.get()
            self._client.publish(
                self._topic_prefix + topic,
                payload=payload,
                qos=self._qos,
                retain=False)

    async def run(self):
        """Run the MQTTPublisher."""
        LOG.debug('MQTTPublisher runs.')
        self._client.connect_async(self._host, port=self._port)
        self._client.loop_start()
        await self._async_helper.wait_for_event(self._is_mqtt_connected)
        await self._keep_publishing()
        self._client.disconnect()
        self._client.loop_stop()
