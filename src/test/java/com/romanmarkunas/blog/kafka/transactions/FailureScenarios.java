package com.romanmarkunas.blog.kafka.transactions;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class FailureScenarios {

    private static final String BROKER = "localhost:9092";
    private static final String TRADE_MATCHER_TOPIC = "orders";
    private static final String AGGREGATOR_TOPIC = "anonymous-orders";
    private static final String GROUP = "test-group";

    private static final List<String> TOPICS = asList(
            TRADE_MATCHER_TOPIC,
            AGGREGATOR_TOPIC);
    private static final List<TopicAndMessage> MESSAGES = asList(
            new TopicAndMessage(TRADE_MATCHER_TOPIC, "id: 123, buy 42 chairs"),
            new TopicAndMessage(AGGREGATOR_TOPIC, "buy 42 chairs"));


    private KafkaProducer<Integer, String> producer;
    private KafkaConsumer<Integer, String> consumer;


    @Before
    public void setup() {
        producer = transactionalProducer();
    }

    @After
    public void teardown() {
        producer.close(2, TimeUnit.SECONDS);
        consumer.close(Duration.ofSeconds(2));
    }


    @Test
    public void noFailuresOccurred_readAllConsumer() {
        CrashableTransactionalKafkaProducer crashableProducer
                = new CrashableTransactionalKafkaProducer(producer);
        crashableProducer.sendTransactionally(MESSAGES);
        crashableProducer.sendTransactionally(MESSAGES);

        consumer = consumer(true);
        ConsumerRecords<Integer, String> records = poll();

        assertEquals(4, records.count());
        printRecordsAndOffset(records, consumer.endOffsets(Collections.emptyList()));
    }

    @Test
    public void noFailuresOccurred_readCommittedConsumer() {
        CrashableTransactionalKafkaProducer crashableProducer
                = new CrashableTransactionalKafkaProducer(producer);
        crashableProducer.sendTransactionally(MESSAGES);
        crashableProducer.sendTransactionally(MESSAGES);

        consumer = consumer(false);
        ConsumerRecords<Integer, String> records = poll();

        assertEquals(4, records.count());
        printRecordsAndOffset(records, consumer.endOffsets(Collections.emptyList()));
    }

    @Test
    public void crashOnSecondTransaction_readAllConsumer() {
        CrashableTransactionalKafkaProducer crashableProducer
                = new CrashableTransactionalKafkaProducer(producer);
        crashableProducer.sendTransactionally(MESSAGES);
        crashableProducer.setShouldCrash(true);
        crashableProducer.sendTransactionally(MESSAGES);

        consumer = consumer(true);
        ConsumerRecords<Integer, String> records = poll();

        assertEquals(3, records.count());
        printRecordsAndOffset(records, consumer.endOffsets(Collections.emptyList()));
    }

    @Test
    public void crashOnSecondTransaction_readCommittedConsumer() {
        CrashableTransactionalKafkaProducer crashableProducer
                = new CrashableTransactionalKafkaProducer(producer);
        crashableProducer.sendTransactionally(MESSAGES);
        crashableProducer.setShouldCrash(true);
        crashableProducer.sendTransactionally(MESSAGES);

        consumer = consumer(false);
        ConsumerRecords<Integer, String> records = poll();

        assertEquals(2, records.count());
        printRecordsAndOffset(records, getOffsets());
    }


    private ConsumerRecords<Integer, String> poll() {
        consumer.subscribe(TOPICS);
        consumer.poll(Duration.ofSeconds(2));
        return consumer.poll(Duration.ofSeconds(2));
    }

    private Map<TopicPartition, Long> getOffsets() {
        return consumer.endOffsets(Collections.emptyList());
    }

    private void printRecordsAndOffset(
            ConsumerRecords<Integer, String> records,
            Map<TopicPartition, Long> offsets) {
        records.forEach(record ->
                System.out.println(record.key() + " : " + record.value()));
        offsets.forEach((partition, offset) ->
                System.out.println(partition + " : " + offset));

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
        // TODO - transaction settings go here

        return new KafkaProducer<>(props);
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
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                readUncomitted ? "read_uncommitted" : "read_committed");

        return new KafkaConsumer<>(props);
    }
}
