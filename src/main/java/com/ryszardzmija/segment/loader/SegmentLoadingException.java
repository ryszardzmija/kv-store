package com.ryszardzmija.segment.loader;

public class SegmentLoadingException extends RuntimeException {
    public SegmentLoadingException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
