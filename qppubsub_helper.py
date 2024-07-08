#!/usr/bin/env python
import datetime, ssl

# https://eclipse.dev/paho/index.php?page=clients/python/index.php
# pip install paho-mqtt
#
# To fix pika packages SSL MacOS issues try:
# Python → Install Certificates.command worked.
# To be exact:
# 1- browse on Mac to Applications > Python 3.11
# 2- double click on “Install Certificates.command”

#
# TODOS:
# API to clear & delete all existing queues on broker
# Secure message publishing / receiving
#

import pika, urllib, requests, json, time

# later this params has to be read from ENV
DEFAULT_BROKER_URL = 'amqps://test:test@msgbroker.qikpod.com:5671/pods'
CERT_FILE = 'ssl/rabbitmq.crt'
KEY_FILE = 'ssl/rabbitmq.key'
CA_FILE = 'ssl/ca.crt'


class PubSubClass:
    def __init__(self, otpic: str, brokerUrl: str = DEFAULT_BROKER_URL):
        self.topic = topic
        self.brokerUrl = brokerUrl
        self.connection = None
        self.channel = None
        self.exchange = "Test_PubSub"
        self.queue_name = None
        self.reconnect_interval = 10  # seconds
        self.connected = False

    def __del__(self):
        try:
            self.connection.close()
        except Exception as e:
            print(f'Exception - __del__: e=[{e}]')
        return

    def connect(self, receiver_mode=True):
        try:
            ssl_context = ssl.create_default_context(cafile=CA_FILE)
            ssl_context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE, password=None)
            ssl_options = pika.SSLOptions(ssl_context)

            parameters = pika.URLParameters(self.brokerUrl)
            parameters.ssl_options = ssl_options

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='topic')
            duration = 3 * 60 * 60 * 1000
            if self.topic[-6:] == "EVENTS":
                self.channel.queue_declare(queue=self.topic, durable=True,
                                           arguments={"x-message-ttl": duration})  # to set TTL to the static queue
            else:
                self.channel.queue_declare(queue=self.topic, durable=True)
            self.channel.queue_bind(exchange=self.exchange, queue=self.topic)

            if receiver_mode:
                result = self.channel.queue_declare('', exclusive=True)
                self.queue_name = result.method.queue
                self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.topic)
            self.connected = True
        except Exception as e:
            print(f'Exception - connect: {e}')
            self.connected = False

    def publish(self, msg_dict: dict) -> bool:
        try:
            if not self.connected:
                self.connect(receiver_mode=False)
            if self.connected:
                if msg_dict is None:
                    msg_dict = {}
                json_body = json.dumps(msg_dict)
                self.channel.basic_publish(exchange=self.exchange, routing_key=self.topic, body=json_body)
                return True
        except Exception as e:
            print(f'Exception - publish: {e}')
            self.connected = False
            return False

    def receive(self, bStaticMode: bool = True) -> dict:
        msg_dict = {}
        try:
            if not self.connected:
                self.connect()
            if self.connected:
                if bStaticMode:
                    method_frame, header_frame, body = self.channel.basic_get(self.topic)
                else:
                    method_frame, header_frame, body = self.channel.basic_get(self.queue_name)
                if method_frame and bStaticMode == False:
                    json_body = str(body.decode())
                    msg_dict = json.loads(json_body)
                    self.channel.basic_ack(method_frame.delivery_tag)
                elif method_frame and bStaticMode:
                    json_body = str(body.decode())
                    msg_dict = json.loads(json_body)

        except Exception as e:
            print(f'Exception - receive: {e}')
            self.connected = False
        return msg_dict

    def receive_all(self, bStaticMode: bool = True) -> list:
        messages = []
        try:
            self.connect()
            count = self.messages_count()["messages"]

            def callback(ch, method, properties, body):
                if len(messages) >= count - 1:
                    print(f"{count}")
                    self.channel.stop_consuming()
                msg_dict = json.loads(body.decode())
                messages.append(msg_dict)

            if count == 0:
                return messages
            self.channel.basic_consume(queue=self.topic, on_message_callback=callback)
            self.channel.start_consuming()
            return messages
        except Exception as e:
            print(f'Exception - receive: {e}')

    def consume(self, bStaticMode: bool = True):
        if not self.connected:
            self.connect()

        def callback(ch, method, properties, body):
            print("Received message:", body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)

        if bStaticMode:
            print(self.channel, self.topic, "True")
            self.channel.basic_consume(queue=self.topic, on_message_callback=callback)
        else:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        print('Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def flush_queue(self, queue_name: str):
        try:
            url_parts = urllib.parse.urlparse(self.brokerUrl)
            host, user, password, vhost = url_parts.hostname, url_parts.username, url_parts.password, url_parts.path[1:] if url_parts.path != '/' else '%2F'
            url = f"https://{host}/api/queues/{vhost}/{queue_name}"
            response = requests.delete(url, auth=(user, password))
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Exception - flushQueue {e}")
            return False

    def flush_exchanges(self, exchange_name: str):
        try:
            url_parts = urllib.parse.urlparse(self.brokerUrl)
            host, user, password, vhost = url_parts.hostname, url_parts.username, url_parts.password, url_parts.path[1:] if url_parts.path != '/' else '%2F'
            url = f"https://{host}/api/exchanges/{vhost}/{exchange_name}"
            response = requests.delete(url, auth=(user, password))
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Exception - flushExchange: {e}")
            return False

    def list_queues(self):
        try:
            url_parts = urllib.parse.urlparse(self.brokerUrl)
            host, user, password, vhost = url_parts.hostname, url_parts.username, url_parts.password, url_parts.path[1:]
            url = f"http://{host}:15672/api/queues/{vhost}"
            response = requests.get(url, auth=(user, password))
            response.raise_for_status()
            queues = response.json()
            queue_names = [queue['name'] for queue in queues]
            return queue_names

        except Exception as e:
            print(f"Exception - list_queues: {e}")
            return []

    def purge_queue(self, queue_name: str):
        try:
            url_parts = urllib.parse.urlparse(self.brokerUrl)
            host, user, password, vhost = url_parts.hostname, url_parts.username, url_parts.password, url_parts.path[1:] if url_parts.path != '/' else '%2F'
            queue_name_encoded = urllib.parse.quote(queue_name)
            print("vhost:",vhost,"queue:",queue_name_encoded)
            url = f"http://{host}:15672/api/queues/{vhost}/{queue_name_encoded}/contents"
            response = requests.delete(url, auth=(user, password))
            response.raise_for_status()
            return True

        except Exception as e:
            print(f"Exception - purge_queue: {e}")
            return False

    def get_message_in_interval(self, start_timestamp=None, end_timestamp=None):
        self.connect()
        response = []

        def callback(ch, method, properties, body):
            msg_dict = json.loads(body.decode())
            msg_timestamp = time.strptime(msg_dict["timestamp"], "%Y-%m-%d %H:%M:%S")
            start_time = time.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
            end_time = time.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S")

            if start_time <= msg_timestamp <= end_time:
                response.append(msg_dict)
            elif msg_timestamp > end_time:
                self.channel.stop_consuming()

        self.channel.basic_consume(queue=self.topic, on_message_callback=callback)
        self.channel.start_consuming()
        return response

    def messages_count(self) -> dict:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(self.brokerUrl))
            channel = connection.channel()
            info = channel.queue_declare(self.topic, passive=True)
            count = info.method.message_count
            connection.close()
            return {"status_code": 200, "messages": count}
        except pika.exceptions.AMQPError as e:
            connection.close()
            return {"status_code": 404, "messages": None}
