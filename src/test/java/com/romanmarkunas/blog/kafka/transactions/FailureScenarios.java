package com.romanmarkunas.blog.kafka.transactions;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.time.LocalTime.now;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FailureScenarios {

    private static final String BROKER = "localhost:9092";
    private static final String TRADE_MATCHER_TOPIC = "orders";
    private static final String AGGREGATOR_TOPIC = "anonymous-orders";
    private static final String GROUP = "test-consumer-group";
    private static final String TRANSACTION_ID = "order-producer";

    private static final List<String> TOPICS = asList(
            TRADE_MATCHER_TOPIC,
            AGGREGATOR_TOPIC);
    private static final List<TopicAndMessage> MESSAGES = asList(
            new TopicAndMessage(TRADE_MATCHER_TOPIC, "id: 123, buy 42 chairs"),
            new TopicAndMessage(AGGREGATOR_TOPIC, "buy 42 chairs"));


    private KafkaProducer<Integer, String> producer;
    private KafkaConsumer<Integer, String> consumer;
    private CrashableTransactionalKafkaProducer crashableProducer;


    @Before
    public void setup() {
        producer = transactionalProducer();
        crashableProducer = new CrashableTransactionalKafkaProducer(producer);
    }

    @After
    public void teardown() {
        if (producer != null) {
            producer.close(2, TimeUnit.SECONDS);
        }
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(2));
        }
    }


    @Test
    public void noFailuresOccurred_readAllConsumer() {
        consumer = consumer(true);
        crashableProducer.sendTransactionally(MESSAGES);
        crashableProducer.sendTransactionally(MESSAGES);

        List<ConsumerRecord<Integer, String>> records = poll();

        assertEquals(4, records.size());
        print(records);
    }

    @Test
    public void noFailuresOccurred_readCommittedConsumer() {
        consumer = consumer(false);
        crashableProducer.sendTransactionally(MESSAGES);
        crashableProducer.sendTransactionally(MESSAGES);

        List<ConsumerRecord<Integer, String>> records = poll();

        assertEquals(4, records.size());
        print(records);
    }

    @Test
    public void crashOnSecondTransaction_readAllConsumer() {
        consumer = consumer(true);
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

    @Test
    public void crashOnSecondTransaction_readCommittedConsumer() {
        consumer = consumer(false);
        crashableProducer.sendTransactionally(MESSAGES);
        crashableProducer.setShouldCrash(true);
        try {
            crashableProducer.sendTransactionally(MESSAGES);
        }
        catch (ForceCrashException e) {
            System.out.println("Producer crashed!");
        }

        List<ConsumerRecord<Integer, String>> records = poll();

        assertEquals(2, records.size());
        print(records);
    }

    @Test
    public void throw_producerWasFencedByAnotherOne() {
        transactionalProducer();
        try {
            crashableProducer.sendTransactionally(MESSAGES);
        }
        catch (ProducerFencedException e) {
            e.printStackTrace();
            return;
        }
        catch (KafkaException e) {
            if (e.getCause() instanceof ProducerFencedException) {
                e.printStackTrace();
                return;
            }
        }

        fail("Did not throw expected ProducerFencedException!");
    }

    
    private List<ConsumerRecord<Integer, String>> poll() {
        List<ConsumerRecord<Integer, String>> records = new ArrayList<>();
        LocalTime stopPolling = now().plus(Duration.ofSeconds(2));

        while (now().isBefore(stopPolling)) {
            ConsumerRecords<Integer, String> batch
                    = consumer.poll(Duration.ofMillis(100));
            batch.forEach(records::add);
        }

        return records;
    }

    private void print(List<ConsumerRecord<Integer, String>> records) {
        records.forEach(record -> System.out.println(
                "Topic: " + record.topic()
              + ", offset: " + record.offset()
              + ", transaction: " + record.key()
              + ", message:  " + record.value()));
    }


    private static Properties minimalClientConfig() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        return props;
    }

    private static KafkaProducer<Integer, String> transactionalProducer() {
        Properties props = minimalClientConfig();
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                TRANSACTION_ID);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        return producer;
    }

    private static KafkaConsumer<Integer, String> consumer(boolean readUncomitted) {
        Properties props = minimalClientConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                readUncomitted ? "read_uncommitted" : "read_committed");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(TOPICS);
        consumer.seekToEnd(Collections.emptyList());
        consumer.poll(Duration.ofMillis(500));
        return consumer;
    }
}
