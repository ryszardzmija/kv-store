package com.ryszardzmija.shaledb.storage.hash.index;

public class IndexBuildException extends RuntimeException {
    public IndexBuildException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
