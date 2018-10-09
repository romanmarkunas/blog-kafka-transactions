package com.romanmarkunas.blog.kafka.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CrashableTransactionalKafkaProducer {

    // TODO - describe caveats with initTransactions + replication
    private final KafkaProducer<Integer, String> producer;

    private AtomicInteger transactionCount = new AtomicInteger(0);
    private boolean shouldCrash = false;


    public CrashableTransactionalKafkaProducer(KafkaProducer<Integer, String> producer) {
        this.producer = producer;
    }


    public void sendTransactionally(List<TopicAndMessage> messages) {
        int transactionNr = transactionCount.incrementAndGet();

        synchronized (producer) {
            producer.beginTransaction();

            for (TopicAndMessage topicAndMessage : messages) {
                producer.send(new ProducerRecord<>(
                        topicAndMessage.topic,
                        transactionNr,
                        topicAndMessage.message));

                if (shouldCrash) {
                    throw new ForceCrashException("Crashed during transaction!");
                }
            }

            producer.commitTransaction();
        }
    }

    public void setShouldCrash(boolean shouldCrash) {
        this.shouldCrash = shouldCrash;
    }


    public static class TopicAndMessage {

        private final String topic;
        private final String message;

        public TopicAndMessage(String topic, String message) {
            this.topic = topic;
            this.message = message;
        }
    }
}
