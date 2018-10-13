# Kafka transactions in practice: Producer

This post will highlight some practical aspects of using kafka transactions. I 
will not dive deep into theory on how Kafka transactions are implemented.  
Instead, let's discuss main "gotchas" someone could encounter when trying to 
use Kafka transactions first time. For detailed theoretical reference please 
refer to [this great article](https://www.confluent.io/blog/transactions-apache-kafka/)

Full code mentioned in article can be found [here](https://github.com/romanmarkunas/blog-kafka-transactions)

## Transactional applications with Kafka

Transactions in Kafka can be divided into 3 big groups: transactions when 
producing messages, transactions when consuming and exactly once processing, 
when both consuming and producing are done within one single transaction.

In this post we will discuss only transactional producing. 

Just one more note before we start. In this article we will discuss only 
transaction semantics, but not delivery semantics. These may differ, especially 
when transaction also involves systems external to Kafka.

## Usage example

Let's say we have an application that allows users to trade chairs between each
other. Users submit buy/sell orders and our app finds matching orders so that 
trades can occur. We also want to aggregate order data to figure out daily 
volumes, most traded instruments and other fancy business intelligence data. 
However we do not want to submit customer specific data to business intelligence 
pipeline because it is personal information and should not be read without 
precautions. One way to tackle that is to have a microservice that forwards 
orders to matcher queue and submits orders stripped of sensitive data into 
aggregator queue. 

## Transactional producer

To submit messages into different queues we use following code:

```java
public void sendTransactionally(List<TopicAndMessage> messages) {
    producer.beginTransaction();
    
    for (TopicAndMessage topicAndMessage : messages) {
        producer.send(new ProducerRecord<>(
                topicAndMessage.getTopic(),
                getTransactionNr(),
                topicAndMessage.getMessage()));

        if (shouldCrash) {
            throw new ForceCrashException("Crashed during transaction!");
        }
    }

    producer.commitTransaction();
}
```

_shouldCrash_ bit is necessary to simulate producer crash to validate 
transaction functionality.

Now lets simulate successful write in our test scenario:

```java
private static final List<TopicAndMessage> MESSAGES = asList(
            new TopicAndMessage(TRADE_MATCHER_TOPIC, "id: 123, buy 42 chairs"),
            new TopicAndMessage(AGGREGATOR_TOPIC, "buy 42 chairs"));

@Test
public void noFailuresOccurred_readAllConsumer() {
    KafkaProducer<Integer, String> producer = ... // create or get transactional producer here
    CrashableTransactionalKafkaProducer crashableProducer
            = new CrashableTransactionalKafkaProducer(producer);
    crashableProducer.sendTransactionally(MESSAGES);
    crashableProducer.sendTransactionally(MESSAGES);

    KafkaConsumer<Integer, String> consumer = ... // create or get consumer here
    ConsumerRecords<Integer, String> records = poll();

    assertEquals(4, records.count());
    printRecordsAndOffset(records, consumer.endOffsets(Collections.emptyList()));
}
```

Output:


Looks good! Lets see how it behaves if crash occurs.

## What if previous example does not work out-of-the-box

Transaction topic and its replication
Transactional producer settings
Commit messages and consumer behavior
Map of TID to PID