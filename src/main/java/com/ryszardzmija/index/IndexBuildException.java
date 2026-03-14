package com.ryszardzmija.index;

public class IndexBuildException extends RuntimeException {
    public IndexBuildException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
