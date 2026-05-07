package com.ryszardzmija.shaledb.storage.durability;

public class DurabilityException extends RuntimeException {
    public DurabilityException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
