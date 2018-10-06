package com.romanmarkunas.blog.kafka.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.atomic.AtomicInteger;

public class CrashableTransactionalKafkaProducer {

   // TODO - describe caveats with initTransactions + replication
   private final KafkaProducer<Integer, String> producer;

   private AtomicInteger transactionCount = new AtomicInteger(0);
   private boolean shouldCrash = false;


   public CrashableTransactionalKafkaProducer(KafkaProducer<Integer, String> producer) {
      this.producer = producer;
   }


   public void sendTransactionally() {
      int transactionNr = transactionCount.incrementAndGet();

      synchronized (producer) {
         producer.beginTransaction();

         producer.send(new ProducerRecord<>("topic1", transactionNr, "msg1"));
         if (shouldCrash) {
            throw new RuntimeException("Crash in the middle of transaction!");
         }
         producer.send(new ProducerRecord<>("topic2", transactionNr, "msg2"));

         producer.commitTransaction();
      }
   }

   public void setShouldCrash(boolean shouldCrash) {
      this.shouldCrash = shouldCrash;
   }
}
