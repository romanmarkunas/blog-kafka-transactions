package com.romanmarkunas.blog.kafka.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CrashableTransactionalKafkaProducerTest {

    private CrashableTransactionalKafkaProducer testProducer;
    private KafkaProducer<Integer, String> kafkaProducerMock;


    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        kafkaProducerMock = mock(KafkaProducer.class);
        testProducer = new CrashableTransactionalKafkaProducer(kafkaProducerMock);
    }


    @Test
    public void sendTransactionally_sendSuccessful() {
        testProducer.sendTransactionally(asList(
                fakeMessage(),
                fakeMessage(),
                fakeMessage()));

        InOrder inOrder = inOrder(kafkaProducerMock);
        inOrder.verify(kafkaProducerMock).beginTransaction();
        inOrder.verify(kafkaProducerMock, times(3)).send(any());
        inOrder.verify(kafkaProducerMock).commitTransaction();
        verifyNoMoreInteractions(kafkaProducerMock);
    }

    @Test
    public void sendTransactionally_doNotCommit_crashAfterFirstSend() {
        testProducer.setShouldCrash(true);
        try {
            testProducer.sendTransactionally(asList(fakeMessage()));
        }
        catch (ForceCrashException e) {
            InOrder inOrder = inOrder(kafkaProducerMock);
            inOrder.verify(kafkaProducerMock).beginTransaction();
            inOrder.verify(kafkaProducerMock).send(any());
            verifyNoMoreInteractions(kafkaProducerMock);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected exception occurred!");
        }
    }


    private TopicAndMessage fakeMessage() {
        return new TopicAndMessage("", "");
    }
}