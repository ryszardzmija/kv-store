package com.ryszardzmija.shaledb.storage.config;

import com.ryszardzmija.shaledb.storage.durability.DurabilityMode;

public record StorageConfigDto(Long maxSegmentSize, Long maxPayloadSize, String segmentDir, DurabilityMode durabilityMode) {}
