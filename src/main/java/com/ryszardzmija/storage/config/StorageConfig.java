package com.ryszardzmija.storage.config;

public record StorageConfig(long maxSegmentSize, long maxRecordSize) {
    public StorageConfig {
        if (maxSegmentSize <= 0) {
            throw new IllegalArgumentException("maxSegmentSize must be positive");
        }
        if (maxRecordSize <= 0 || maxRecordSize > maxSegmentSize) {
            throw new IllegalArgumentException("maxRecordSize must be positive and less than maxSegmentSize");
        }
    }
}
