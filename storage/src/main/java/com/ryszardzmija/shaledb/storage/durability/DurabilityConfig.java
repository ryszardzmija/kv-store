package com.ryszardzmija.shaledb.storage.durability;

import com.ryszardzmija.shaledb.storage.config.StorageConfig;

public record DurabilityConfig(DurabilityMode durabilityMode) {
    public static DurabilityConfig from(StorageConfig config) {
        return new DurabilityConfig(config.durabilityMode());
    }
}
