package com.ryszardzmija.storage.hash.segment.rollover;

public class RolloverException extends RuntimeException {
    public RolloverException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
