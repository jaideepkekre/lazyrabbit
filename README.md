# easymq
An stupidly easy wrapper over the PIKA amqp package.

# Send a message 

```python
mq = EasyMQ(queueName,exchangeName)
mq.ADD_OR_GET(inputDict)
```

