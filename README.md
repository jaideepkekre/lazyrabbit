# LazyRabbit for RabbitMQ
An stupidly easy wrapper over the Pika amqp package for RabbitMQ for the lazy python dev.

pika-amqp : https://github.com/pika/pika

## Send a message 

```python
val = dict()
val["test"] = "value"    
mq = LazyRabbit("testq1")
mq.ADD_OR_GET(val)
# Only dict input allowed
```

This library also now auto recovers connections if the main thread is busy,
and cannot communicate with MQ broker to establish heartbeat.This is observed with long 
running tasks that output to a queue.



## Get a message 

```python
mq = LazyRabbit("testq1")
dict_from_mq = mq.ADD_OR_GET()
#returns None if queue is empty
 ```


 ## Settings for class object 
 ### (you really don't need to look at this if you're lazy)


* queue : The name of the queue you want to send to. (No defaults)
* exchange= The exchange used to route the message to above queue (default : 'LazyRabbit')
* ip= The IP on which the RabbitMQ is running (default :'localhost')
* exchange_type=Type of exchange declared in 'exchange' arg , can be 'direct' or 'fanout'  (default: 'direct') 
* log_level='INFO'
* send=True (True|False) if to create a sender connection
* get=True  (True|False) if to create a getter connection
* getter_wait= The time to sleep if no message if found in queue (default : 0.1s)
* getter_wait_long= The time to sleep if no message if found in queue 1000  times or greater consecutively  (default : 10s)


