package com.ryszardzmija.shaledb.storage.serialization.io;

public class DataCorruptionException extends RuntimeException {
    private static final String ERROR_MESSAGE_TEMPLATE = "Data corruption detected at offset %d: %s";

    public DataCorruptionException(String message, long offset) {
        super(ERROR_MESSAGE_TEMPLATE.formatted(offset, message));
    }

    public DataCorruptionException(String message, long offset, Throwable throwable) {
        super(ERROR_MESSAGE_TEMPLATE.formatted(offset, message), throwable);
    }
}
