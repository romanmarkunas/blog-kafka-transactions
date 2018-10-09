package com.romanmarkunas.blog.kafka.transactions;

public class ForceCrashException extends RuntimeException {

    public ForceCrashException(String message) {
        super(message);
    }
}
