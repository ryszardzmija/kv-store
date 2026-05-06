package com.ryszardzmija.shaledb.storage.config;

public record StorageConfigDto(Long maxSegmentSize, Long maxPayloadSize, String segmentDir) {}
