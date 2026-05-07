package com.ryszardzmija.shaledb.storage.config;

import com.ryszardzmija.shaledb.storage.durability.DurabilityMode;

import java.nio.file.Path;

public record StorageConfig(long maxSegmentSize, long maxPayloadSize, Path segmentDir, DurabilityMode durabilityMode) {
    public StorageConfig {
        if (maxSegmentSize <= 0) {
            throw new IllegalArgumentException("maxSegmentSize must be greater than 0 bytes, but was " + maxSegmentSize);
        }

        if (maxPayloadSize <= 0) {
            throw new IllegalArgumentException("maxPayloadSize must be greater than 0 bytes, but was " + maxPayloadSize);
        }

        if (maxPayloadSize > maxSegmentSize) {
            throw new IllegalArgumentException("maxPayloadSize must not exceed maxSegmentSize: maxPayloadSize=" +
                    maxPayloadSize + ", maxSegmentSize=" + maxSegmentSize);
        }

        if (segmentDir == null) {
            throw new IllegalArgumentException("segmentDir must not be null");
        }

        if (durabilityMode == null) {
            throw new IllegalArgumentException("durabilityMode must not be null");
        }
    }
}
