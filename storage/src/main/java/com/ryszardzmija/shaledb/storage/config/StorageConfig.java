package com.ryszardzmija.shaledb.storage.config;

import java.nio.file.Path;
import java.util.Objects;

public record StorageConfig(long maxSegmentSize, long maxRecordSize, Path segmentDir) {
    public StorageConfig {
        if (maxSegmentSize <= 0) {
            throw new IllegalArgumentException("maxSegmentSize must be positive");
        }
        if (maxRecordSize <= 0 || maxRecordSize > maxSegmentSize) {
            throw new IllegalArgumentException("maxRecordSize must be positive and less than maxSegmentSize");
        }
        Objects.requireNonNull(segmentDir);
    }
}
