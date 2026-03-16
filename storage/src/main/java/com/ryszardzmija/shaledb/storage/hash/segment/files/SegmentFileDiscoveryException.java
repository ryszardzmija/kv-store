package com.ryszardzmija.shaledb.storage.hash.segment.files;

/**
 * Thrown after encountering an error during segment file discovery.
 */
public class SegmentFileDiscoveryException extends RuntimeException {
    public SegmentFileDiscoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
