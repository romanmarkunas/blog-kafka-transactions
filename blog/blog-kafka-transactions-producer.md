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
pipeline because it is personal information and should not be read by unauthorized  
personnel. One way to tackle that is to have a microservice that forwards 
orders to matcher queue and submits orders stripped of sensitive data into 
aggregator queue. 

## Successful scenario

To submit messages into different queues we use following code:

```java
public void sendTransactionally(List<TopicAndMessage> messages) {
    producer.beginTransaction();
    
    for (TopicAndMessage topicAndMessage : messages) {
        producer.send(new ProducerRecord<>(
                topicAndMessage.getTopic(),
                getTransactionNr(),
                topicAndMessage.getMessage()));
    }

    producer.commitTransaction();
}
```

Now lets simulate successful write in our test scenario:

```java
private static final List<TopicAndMessage> MESSAGES = asList(
            new TopicAndMessage(TRADE_MATCHER_TOPIC, "id: 123, buy 42 chairs"),
            new TopicAndMessage(AGGREGATOR_TOPIC, "buy 42 chairs"));

private KafkaConsumer<Integer, String> consumer = ... // create or get read_uncommitted consumer here
private KafkaProducer<Integer, String> producer = ... // create or get transactional producer here

@Test
public void noFailuresOccurred_readAllConsumer() {
    CrashableTransactionalKafkaProducer crashableProducer
            = new CrashableTransactionalKafkaProducer(producer);
    crashableProducer.sendTransactionally(MESSAGES);
    crashableProducer.sendTransactionally(MESSAGES);

    ConsumerRecords<Integer, String> records = poll();

    assertEquals(4, records.count());
    print(records);
}
```

Output:
```
Topic: orders, offset: 0, transaction: 1, message:  id: 123, buy 42 chairs
Topic: anonymous-orders, offset: 0, transaction: 1, message:  buy 42 chairs
Topic: orders, offset: 2, transaction: 2, message:  id: 123, buy 42 chairs
Topic: anonymous-orders, offset: 2, transaction: 2, message:  buy 42 chairs
```

Looks good! Very attentive reader will notice that 2nd transaction offsets 
start from 2 even though we submitted only 1 message before that. Also what does
_read_uncommitted_ mean in comment above? Let's discuss this in a moment after 
checking out failure scenario.

## Transactional KafkaProducer initialisation is stuck 

If something is set incorrectly, most of the time Kafka will throw descriptive 
exception, for example:

`java.lang.IllegalStateException: Cannot use transactional methods without 
enabling transactions by setting the transactional.id configuration property`

However one setting may cause indefinite blocking and "hanging". This is broker 
setting _transaction.state.log.min.isr_ (see also 
_transaction.state.log.replication.factor_ when changing this). To keep track of 
which transactions were committed Kafka keeps internal topic called 
___transaction_state_. This topic should be replicated for fault-tolerance as 
any other with Kafka. Now when you call _initTransactions()_ on producer, this 
call will block until topic has all in-sync replicas acknowledgement. 

For example, if broker has only 2 machines online and  
_transaction.state.log.min.isr = 3_, _initTransactions()_ will block until one 
more broker node gets online. If that 3rd node will go offline after producer 
has been initialized successfully, everything will keep working. However for 
startup it is necessary to have minimum broker node count online.

## Failure scenario

Now following scenario:

```java
@Test
public void crashOnSecondTransaction_readAllConsumer() {
    crashableProducer.sendTransactionally(MESSAGES);
    crashableProducer.setShouldCrash(true);
    try {
        crashableProducer.sendTransactionally(MESSAGES);
    }
    catch (ForceCrashException e) {
        System.out.println("Producer crashed!");
    }

    List<ConsumerRecord<Integer, String>> records = poll();

    assertEquals(3, records.size());
    print(records);
}
```

gives us:

```
Producer crashed!
Topic: orders, offset: 0, transaction: 1, message:  id: 123, buy 42 chairs
Topic: anonymous-orders, offset: 0, transaction: 1, message:  buy 42 chairs
Topic: orders, offset: 2, transaction: 2, message:  id: 123, buy 42 chairs
```

Oops! Looks like transactions didn't work. But they actually do. The problem 
here is that consumer was created with _isolation.level = read_uncommitted_ 
setting, which is it's default value. Every time producer commits a transaction 
it submits a commit marker to the log. If consumer's _isolation.level_ is set
to _read_uncommitted_ it just reads all messages it can. In case setting is 
_read_committed_ consumer will wait until commit marker is submitted to decide
what to do with preceding messages.

This commit marker contains all necessary data for consumer to read from 
___transactions_state_ topic and decide, if message must be returned. In case when 
producer abort transaction commit marker will represents abort and will be 
ignore by consumer. In case producer dies, broker will ultimately expire 
transaction and abort marker itself. This is why if you submit _n_ messages to 
topic the next batch will always start at _n+1_ offset.

Now if we set consumer's _isolation.level = read_committed_, we will get:

```
Producer crashed!
Topic: orders, offset: 4, transaction: 1, message:  id: 123, buy 42 chairs
Topic: anonymous-orders, offset: 2, transaction: 1, message:  buy 42 chairs
``` 

So important note here is that transactional producing is dependent on the 
readers. Therefore when building pipelines with Kafka, every node in chain 
settings must be carefully considered, because one error may cause change in all 
pipeline delivery semantics!

## Producer fencing and expiration

To protect cluster from intermittent connection errors Kafka uses epochs tied 
to producer's transaction ID. Every time producer calls _initTransactions()_, 
cluster will assign it an _epoch = previous_epoch + 1_. This means that if 
you start producer with transaction id and cluster has active producer with 
same id, the latter one will be disconnected (fenced in Kafka terms) and throw 
an exception next time it tries to send message:

```
org.apache.kafka.common.errors.ProducerFencedException: Producer attempted an 
operation with an old epoch. Either there is a newer producer with the same 
transactionalId, or the producer's transaction has been expired by the broker.
```

Sometimes producer throws another exception, with ProducerFencedException as 
cause.

But this exception may occur even without connectivity errors. Internally 
broker keeps a map of producer IDs to transaction IDs to maintain aforementioned 
fencing functionality. However broker will expire these entries based on 
_transactional.id.expiration.ms_ setting which is by default 604800000 ms or 
1 week. 

This means that if your producer sends messages slower than once a week it will 
be fenced (crash) every time it tries to send after that prolonged period. One 
could put that value to Integer.MAX, but that still will be around 24 days. In 
case of very rare events, this should be solved differently, e.g. by having 
separate producer ping topic, that producer will periodically send messages to.

## Instead of conclusion

That's it for Kafka producers! Hopefully this will help you to understand, setup 
Kafka transactions and successfully keep them producing. For more implementation 
details how Kafka transactions work, I recommend blog mentioned in this post's 
intro and this [Kafka KIP](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging#KIP-98-ExactlyOnceDeliveryandTransactionalMessaging-2.GettingaproducerId--theInitPidRequest).

Stay tuned for article on transactional consuming!
