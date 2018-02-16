#!/usr/bin/python
"""
jaideep@capiot.com
"""
import json
import logging
import time
import pika


class LazyRabbit(object):
    """
    Creates an EasyMQ object with setter / getter connections.
    refer to readme for configuration args. 
    """

    def __init__(self,
                 queue,
                 exchange='LazyRabbit',
                 ip='localhost',
                 exchange_type='direct',
                 log_level='INFO',
                 send=True,
                 get=True,
                 getter_wait=0.1,
                 getter_wait_long=10):

        self.QUEUE = queue
        self.EXCHANGE = exchange
        self.IP = ip
        self.EXCHANGE_TYPE = exchange_type
        self.log_level = log_level
        self.greedy = 1
        if send:
            self.SEND_CONNECTION = pika.BlockingConnection(
                pika.ConnectionParameters(ip))
            self.SEND_CHANNEL = self._setup_connection(self.SEND_CONNECTION)
            self.SEND_CHANNEL.confirm_delivery()
        if get:
            self.GET_CONNECTION = pika.BlockingConnection(
                pika.ConnectionParameters(ip))
            self.GET_CHANNEL, _ = self._create_default_channel(
                self.GET_CONNECTION)
            self.GETTER_WAIT = getter_wait
            self.GETTER_WAIT_LONG = getter_wait_long

    def _create_default_channel(self, connection):
        """Create a channel and a declare a queue."""
        channel = connection.channel()
        # declare queue
        result = channel.queue_declare(queue=self.QUEUE)
        self.created_queue = result
        return channel, result

    def _setup_connection(self, connection):
        """ Create a new connection."""
        channel, result = self._create_default_channel(connection)
        if self.EXCHANGE is '':
            return channel

        # declare exchange
        channel.exchange_declare(
            exchange=self.EXCHANGE, exchange_type=self.EXCHANGE_TYPE)
        queue_name = result.method.queue
        # bind queue to exchange
        channel.queue_bind(exchange=self.EXCHANGE, queue=queue_name)
        return channel

    def __actually_get(self):
        # print (self.greedy)

        method_frame, header_frame, body = self.GET_CHANNEL.basic_get(
            self.QUEUE)

        if body is None:
            return None

        response = json.loads(body)
        if header_frame.headers:
            response["headers"] = json.loads(header_frame.headers)
        else:
            response["headers"] = None

        if method_frame is not None:
            self.GET_CHANNEL.basic_ack(method_frame.delivery_tag)
        return response

    def _getmsg(self):
        """
        GETS A MESSAGE FROM QUEUE IN DICT FORM
        """
        logging.basicConfig(level=self.log_level)
        logger = logging.getLogger("_ADDMSG")

        if len(self.QUEUE) is 0:
            logger.error("QUEUE cannot be empty if in get mode")
            raise ValueError

        if self.QUEUE is None:
            logger.error("QUEUE cannot be None if in get mode")
            raise ValueError

        return self.__actually_get()
        if response:
            self.greedy = 0
            return self.__actually_get()
        else:
            self.greedy += 1
            if self.greedy > 1000:
                time.sleep(self.GETTER_WAIT_LONG)
            time.sleep(self.GETTER_WAIT)
            return response

    def _addmsg(self, message_dict):
        """
        ADD MESSAGE TO SEND CHANNEL IN JSON FORMAT
        """
        logging.basicConfig(level=self.log_level)
        logger = logging.getLogger("_ADDMSG")

        if not type(message_dict) == type(dict()):
            logger.error("message_dict should be of type dict not :" +
                         str(type(message_dict)))
            raise TypeError

        if len(message_dict) is 0:
            logger.error("message_dict cannot be empty if in send mode")
            raise ValueError

        if message_dict is None:
            logger.error("message_dict cannot be None if in send mode")
            raise ValueError
        try:
            self.SEND_CHANNEL.publish(
                exchange=self.EXCHANGE,
                routing_key=self.QUEUE,
                body=json.dumps(message_dict),
                mandatory=True)
        except pika.exceptions.ConnectionClosed:
            self.SEND_CONNECTION = pika.BlockingConnection(
                pika.ConnectionParameters(self.IP))
            self.SEND_CHANNEL = self._setup_connection(self.SEND_CONNECTION)
            self.SEND_CHANNEL.confirm_delivery()

        return True

    def add_or_get(self, message_dict=None):
        """
        SEND/GET MESSAGE TO/FROM A EXCHANGE/QUEUE VIA CHANNEL, 
        IF IN SEND , ONLY DICTS ACCEPTED,
        IF IN GET  , ONLY DICTS RETURNED
        """

        logging.basicConfig(level=self.log_level)
        logger = logging.getLogger("send_message")

        try:
            if message_dict is not None:
                self._addmsg(message_dict)
                logger.info("Sending message: " + str(message_dict))
            else:
                return self._getmsg()
                logger.info("Attempting to get message")
        except Exception:
            raise


def tester():

    val = dict()
    val["test"] = "value"
    mq = LazyRabbit("Queue-1", get=False)
    while True:
        res = mq.add_or_get(val)
        time.sleep(100)


if __name__ == '__main__':
    tester()
