package com.ryszardzmija.shaledb.storage;

public class StorageEngineException extends RuntimeException {
    public StorageEngineException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
