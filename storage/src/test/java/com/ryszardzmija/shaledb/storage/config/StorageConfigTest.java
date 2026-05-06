package com.ryszardzmija.shaledb.storage.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageConfigTest {
    private static final long VALID_MAX_SEGMENT_SIZE = 65536;
    private static final long VALID_MAX_PAYLOAD_SIZE = 16384;

    @TempDir
    Path tempDir;

    @Test
    void acceptsValidConfig() {
        long maxSegmentSize = VALID_MAX_SEGMENT_SIZE;
        long maxPayloadSize = VALID_MAX_PAYLOAD_SIZE;

        StorageConfig config = new StorageConfig(maxSegmentSize, maxPayloadSize, tempDir);

        assertThat(config.maxSegmentSize()).isEqualTo(maxSegmentSize);
        assertThat(config.maxPayloadSize()).isEqualTo(maxPayloadSize);
        assertThat(config.segmentDir()).isEqualTo(tempDir);
    }

    @Test
    void rejectsZeroMaxSegmentSize() {
        long maxSegmentSize = 0;

        assertThatThrownBy(() -> new StorageConfig(maxSegmentSize, VALID_MAX_PAYLOAD_SIZE, tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxSegmentSize must be greater than 0 bytes, but was " + maxSegmentSize);
    }

    @Test
    void rejectsNegativeMaxSegmentSize() {
        long maxSegmentSize = -1;

        assertThatThrownBy(() -> new StorageConfig(maxSegmentSize, VALID_MAX_PAYLOAD_SIZE, tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxSegmentSize must be greater than 0 bytes, but was " + maxSegmentSize);
    }

    @Test
    void rejectsZeroMaxPayloadSize() {
        long maxPayloadSize = 0;

        assertThatThrownBy(() -> new StorageConfig(VALID_MAX_SEGMENT_SIZE, maxPayloadSize, tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPayloadSize must be greater than 0 bytes, but was " + maxPayloadSize);
    }

    @Test
    void rejectsNegativeMaxPayloadSize() {
        long maxPayloadSize = -1;

        assertThatThrownBy(() -> new StorageConfig(VALID_MAX_SEGMENT_SIZE, maxPayloadSize, tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPayloadSize must be greater than 0 bytes, but was " + maxPayloadSize);
    }

    @Test
    void rejectsMaxPayloadSizeExceedingMaxSegmentSize() {
        long maxPayloadSize = VALID_MAX_SEGMENT_SIZE + 1;

        assertThatThrownBy(() -> new StorageConfig(VALID_MAX_SEGMENT_SIZE, maxPayloadSize, tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPayloadSize must not exceed maxSegmentSize: maxPayloadSize=" +
                        maxPayloadSize + ", maxSegmentSize=" + VALID_MAX_SEGMENT_SIZE);
    }

    @Test
    void rejectsNullSegmentDir() {
        assertThatThrownBy(() -> new StorageConfig(VALID_MAX_SEGMENT_SIZE, VALID_MAX_PAYLOAD_SIZE, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("segmentDir must not be null");
    }
}
