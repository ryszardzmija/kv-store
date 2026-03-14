package com.ryszardzmija.segment.files;

/**
 * Thrown after encountering an unrecoverable file management operation for segment files.
 */
public class SegmentFileException extends RuntimeException {
    public SegmentFileException(String message) {
        super(message);
    }

    public SegmentFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
