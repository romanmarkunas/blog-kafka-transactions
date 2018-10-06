package com.romanmarkunas.blog.kafka.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;

public class CrashableTransactionalKafkaProducer {

   private final KafkaProducer<Integer, String> kafkaProducer;
   private boolean crashDuringTransaction;



}
