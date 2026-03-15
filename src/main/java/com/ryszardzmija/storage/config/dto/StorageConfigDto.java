package com.ryszardzmija.storage.config.dto;

import com.ryszardzmija.storage.config.StorageConfig;

public class StorageConfigDto {
    public long maxSegmentSize;
    public long maxRecordSize;

    public StorageConfig toStorageConfig() {
        return new StorageConfig(maxSegmentSize, maxRecordSize);
    }
}
