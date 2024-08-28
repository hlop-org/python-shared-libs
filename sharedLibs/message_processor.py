import sys, os
import time
from datetime import datetime
import logging
import json
from threading import Thread
import traceback
import pika

logger = logging.getLogger(__name__)


class Producer:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.vhost = kwargs.get('vhost', '/')
        self.port = kwargs.get('port', 5672)
        self.user = kwargs.get('user', 'guest')
        self.password = kwargs.get('password', 'guest')
        self.exchange = kwargs.get('exchange', '')
        self.routing_key = kwargs.get('routing_key')
        if not self.routing_key:
            raise Exception('Queue name is required')
        self.__connect()

    def __connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(self.host, self.port, self.vhost, credentials)
        self.__connection = pika.BlockingConnection(parameters)
        self.__channel = self.__connection.channel()


    def close(self):
        self.__connection.close()


    def publish(self, msg):
        try:
            logger.debug(f"Publishing {msg}")
            self.__channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key, body=msg)
        except Exception as e:
            logger.error(f"Error publishing message: {traceback.format_exc()}")


class ThreadedConsumer:

    def __init__(self, host, port, user, password, queue, on_message, on_connect, **kwargs):
        self.host = host
        self.port = port
        self.vhost = kwargs.get('vhost', '/')
        self.user = user
        self.password = password
        self.queue = queue
        self.__daemon_process = kwargs.get('daemon_process', True)
        self.client_name = kwargs.get('client_name', 'consumer')
        self.auto_ack = kwargs.get('auto_ack', False)
        self.__prefetch_count = kwargs.get('prefetch_count', 10)
        self.__on_message_callback = on_message
        self.__on_connect_callback = on_connect

        self.thread = Thread(target=self.__worker, daemon=self.__daemon_process)


    def __worker(self):
        while True:
            try:
                self.__connect()
                logger.info('Connected')
                self.channel.start_consuming()
            except Exception as e:
                logger.error(f"Error: {traceback.format_exc()}")
                continue
            finally:
                logger.info('Waiting for 5 seconds before reconnect...')
                time.sleep(5)


    def start(self):
        self.thread.start()


    def __on_message(self, channel, method, properties, body):
        self.__on_message_callback(channel, method, properties, body)


    def __connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        client_properties = { 'connection_name' : self.client_name }
        parameters = pika.ConnectionParameters(self.host,
                                               self.port,
                                               self.vhost,
                                               credentials,
                                               heartbeat=0,
                                               client_properties=client_properties)

        logger.debug(f"Connecting to {self.host}:{self.port}{self.vhost} as {self.client_name}")
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=self.__prefetch_count)
        logger.info('Connected')

        logger.debug('Calling on_connect callback')
        self.__on_connect_callback(self.channel)

        self.channel.basic_consume(queue=self.queue,
                                   on_message_callback=self.__on_message,
                                   auto_ack=self.auto_ack)


