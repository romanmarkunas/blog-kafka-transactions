package com.romanmarkunas.blog.kafka.transactions;

public class TopicAndMessage {

    private final String topic;
    private final String message;

    public TopicAndMessage(String topic, String message) {
        this.topic = topic;
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessage() {
        return message;
    }
}
