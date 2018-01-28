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



## Get a message 

```python
mq = LazyRabbit("testq1")
dict_from_mq = mq.ADD_OR_GET()
#returns None if queue is empty
 ```

