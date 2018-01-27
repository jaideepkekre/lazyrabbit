# easymq
An stupidly easy wrapper over the Pika amqp package for RabbitMQ.

## Send a message 

```python
val = dict()
val["test"] = "value"    
mq = EasyMQ("testq1","testex1")
mq.ADD_OR_GET(val)
# Only dict input allowed
```    



## Get a message 

```python
mq = EasyMQ("testq1","testex1")
dict_from_mq = mq.ADD_OR_GET()
#returns None if queue is empty
 ```

