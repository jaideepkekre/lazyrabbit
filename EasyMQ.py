#!/usr/bin/python
"""
jaideep@capiot.com
"""
import json
import logging
import time
import pika


class EasyMQ(object):
    def __init__(self,
                 queue,
                 exchange='',
                 ip='localhost',
                 exchange_type='direct',
                 log_level='INFO',
                 send=True,
                 get=True,
                 getter_wait=0.1,
                 getter_wait_long=10):
        self.CONNECTION = pika.BlockingConnection(
            pika.ConnectionParameters(ip))
        self.QUEUE = queue
        self.EXCHANGE = exchange
        self.IP = ip
        self.EXCHANGE_TYPE = exchange_type
        self.log_level = log_level
        self.greedy = 1
        if send:
            self.SEND_CHANNEL = self._setup_connection()
            self.SEND_CHANNEL.confirm_delivery()
        if get:
            self.GET_CHANNEL, _ = self._create_default_channel()
            self.GETTER_WAIT = getter_wait
            self.GETTER_WAIT_LONG = getter_wait_long

    def _create_default_channel(self):
        """
        creates a channel and a declares a queue
        """

        channel = self.CONNECTION.channel()
        # declare queue
        result = channel.queue_declare(queue=self.QUEUE)
        self.created_queue = result
        return channel, result

    def _setup_connection(self):
        """
        create a new connection
        """

        channel, result = self._create_default_channel()
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

        if not body:
            return None
        response = dict()

        if header_frame.headers:
            response["headers"] = json.loads(header_frame.headers)
        else:
            response["headers"] = None

        response["body"] = json.loads(body)
        self.GET_CHANNEL.basic_ack(method_frame.delivery_tag)
        return response

    def _GETMSG(self):
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

        response = self.__actually_get()
        if response:
            self.greedy = 0
            return self.__actually_get()
        else:
            self.greedy += 1
            if self.greedy > 1000:
                time.sleep(self.GETTER_WAIT_LONG)
            time.sleep(self.GETTER_WAIT)
            return response

    def _ADDMSG(self, MESSAGE_DICT):
        """
        ADD MESSAGE TO SEND CHANNEL IN JSON FORMAT
        """
        logging.basicConfig(level=self.log_level)
        logger = logging.getLogger("_ADDMSG")

        if not type(MESSAGE_DICT) == type(dict()):
            logger.error("MESSAGE_DICT should be of type dict not :" +
                         str(type(MESSAGE_DICT)))
            raise TypeError

        if len(MESSAGE_DICT) is 0:
            logger.error("MESSAGE_DICT cannot be empty if in send mode")
            raise ValueError

        if MESSAGE_DICT is None:
            logger.error("MESSAGE_DICT cannot be None if in send mode")
            raise ValueError

        self.SEND_CHANNEL.publish(
            exchange=self.EXCHANGE,
            routing_key=self.QUEUE,
            body=json.dumps(MESSAGE_DICT),
            mandatory=True)

        return True

    def ADD_OR_GET(self, MESSAGE_DICT=None):
        """
        SEND/GET MESSAGE TO/FROM A EXCHANGE/QUEUE VIA CHANNEL, 
        IF IN SEND , ONLY DICTS ACCEPTED,
        IF IN GET  , ONLY DICTS RETURNED
        """

        logging.basicConfig(level=self.log_level)
        logger = logging.getLogger("send_message")

        try:
            if MESSAGE_DICT is not None:
                self._ADDMSG(MESSAGE_DICT)
                logger.info("Sending message: " + str(MESSAGE_DICT))
            else:
                return self._GETMSG()
                logger.info("Attempting to get message")

        except Exception:
            raise


def tester():

    val = dict()
    val["test"] = "value"
    mq = EasyMQ("testq1", "testex1")
    while True:
        mq.ADD_OR_GET(val)
        print (mq.ADD_OR_GET())


if __name__ == '__main__':
    tester()
