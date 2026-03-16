package com.ryszardzmija.shaledb.storage.serialization.io;

import com.ryszardzmija.shaledb.storage.config.StorageConfigHolder;

public record ReadRequest(long offset) {
    public ReadRequest {
        long maxSegmentSize = StorageConfigHolder.get().maxSegmentSize();
        if (offset < 0 || offset > maxSegmentSize) {
            throw new IllegalArgumentException("offset must be non-negative and within the maximum size of a segment: " + maxSegmentSize);
        }
    }
}
