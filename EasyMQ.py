#!/usr/bin/python
"""
jaideep@capiot.com
"""
import pika
import json
import types

import logging


class EasyMQ(object):

    def __init__(self,queue,exchange='',ip='localhost',exchange_type='direct',log_level='INFO',send=True,get=True):

        self.QUEUE=queue
        self.EXCHANGE=exchange
        self.IP=ip
        self.EXCHANGE_TYPE=exchange_type
        self.log_level = log_level
        if send:
            self.SEND_CHANNEL = self._setup_connection()
            self.SEND_CHANNEL.confirm_delivery()            
        if get : 
            self.GET_CHANNEL,_= self._create_default_channel()
        


    def _create_default_channel(self):
        """
        creates a channel and a declares a queue
        """
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.IP))
        channel = connection.channel()
        # declare queue
        result = channel.queue_declare(queue=self.QUEUE)
        return channel , result




    def _setup_connection(self):
        """
        create a new connection
        """

        channel,result =self._create_default_channel()   
        if self.EXCHANGE is '':
            return channel

        # declare exchange
        channel.exchange_declare(exchange=self.EXCHANGE,
                                exchange_type=self.EXCHANGE_TYPE)
        queue_name = result.method.queue
        # bind queue to exchange
        channel.queue_bind(exchange=self.EXCHANGE,
                        queue=queue_name)
        return channel


    def _GETMSG(self):
        """
        GETS A MESSAGE FROM QUEUE IN DICT FORM
        """
        logging.basicConfig(level=self.log_level)
        logger = logging.getLogger("_ADDMSG")

        if len(self.QUEUE) is 0:
            logger.error(
                "QUEUE cannot be empty if in get mode")
            raise ValueError

        if self.QUEUE is None:
            logger.error(
                "QUEUE cannot be None if in get mode")
            raise ValueError
        
        method_frame, header_frame, body = self.GET_CHANNEL.basic_get(self.QUEUE)
        if method_frame:
            self.GET_CHANNEL.basic_ack(method_frame.delivery_tag)
        else:
            return None
        return json.loads(body)


    def _ADDMSG(self,MESSAGE_DICT):
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
            logger.error(
                "MESSAGE_DICT cannot be empty if in send mode")
            raise ValueError

        if MESSAGE_DICT is None:
            logger.error(
                "MESSAGE_DICT cannot be None if in send mode")
            raise ValueError  


       

        self.SEND_CHANNEL.publish(exchange=self.EXCHANGE, routing_key=self.QUEUE,
                        body=json.dumps(MESSAGE_DICT),mandatory=True)       

        return True


    def ADD_OR_GET(self,MESSAGE_DICT=None):
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
            else:
                return self._GETMSG()

        except Exception:
            raise


def tester():

    val = dict()
    val["test"] = "value"    
    mq = EasyMQ("testq1","testex1")
    while True:
        mq.ADD_OR_GET(val)
        #print (mq.ADD_OR_GET())
    


if __name__ == '__main__':
    tester()
