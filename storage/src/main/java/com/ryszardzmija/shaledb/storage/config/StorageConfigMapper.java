package com.ryszardzmija.shaledb.storage.config;

import java.nio.file.Path;

public class StorageConfigMapper {
    public StorageConfig toStorageConfig(StorageConfigDto storageConfigDto) {
        if (storageConfigDto == null) {
            throw new StorageConfigException("Missing required configuration section: storage");
        }

        try {
            return new StorageConfig(
                    require(storageConfigDto.maxSegmentSize(), "storage.maxSegmentSize"),
                    require(storageConfigDto.maxPayloadSize(), "storage.maxPayloadSize"),
                    Path.of(require(storageConfigDto.segmentDir(), "storage.segmentDir")),
                    require(storageConfigDto.durabilityMode(), "storage.durabilityMode")
            );
        } catch (IllegalArgumentException e) {
            throw new StorageConfigException("Invalid storage configuration", e);
        }
    }

    private <T> T require(T value, String fieldName) {
        if (value == null) {
            throw new StorageConfigException("Missing required configuration field: " + fieldName);
        }

        return value;
    }
}
