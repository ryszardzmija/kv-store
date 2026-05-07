package com.ryszardzmija.shaledb.storage.config;

import com.ryszardzmija.shaledb.storage.durability.DurabilityMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageConfigMapperTest {
    private static final long VALID_MAX_SEGMENT_SIZE = 65536;
    private static final long VALID_MAX_PAYLOAD_SIZE = 16384;
    private static final String VALID_SEGMENT_DIR = "data/segments";
    private static final DurabilityMode VALID_DURABILITY_MODE = DurabilityMode.SYNC_EACH_WRITE;

    StorageConfigMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new StorageConfigMapper();
    }

    @Test
    void correctlyMapsValidDtoToConfig() {
        StorageConfigDto config = new StorageConfigDto(VALID_MAX_SEGMENT_SIZE, VALID_MAX_PAYLOAD_SIZE, VALID_SEGMENT_DIR, VALID_DURABILITY_MODE);

        StorageConfig mappedConfig = mapper.toStorageConfig(config);

        assertThat(mappedConfig.maxSegmentSize()).isEqualTo(VALID_MAX_SEGMENT_SIZE);
        assertThat(mappedConfig.maxPayloadSize()).isEqualTo(VALID_MAX_PAYLOAD_SIZE);
        assertThat(mappedConfig.segmentDir()).isEqualTo(Path.of(VALID_SEGMENT_DIR));
        assertThat(mappedConfig.durabilityMode()).isEqualTo(VALID_DURABILITY_MODE);
    }

    @Test
    void rejectsMissingStorageSection() {
        assertThatThrownBy(() -> mapper.toStorageConfig(null))
                .isInstanceOf(StorageConfigException.class)
                .hasMessage("Missing required configuration section: storage");
    }

    @Test
    void rejectsMissingMaxSegmentSize() {
        String fieldName = "storage.maxSegmentSize";
        StorageConfigDto config = new StorageConfigDto(null, VALID_MAX_PAYLOAD_SIZE, VALID_SEGMENT_DIR, VALID_DURABILITY_MODE);

        assertThatThrownBy(() -> mapper.toStorageConfig(config))
                .isInstanceOf(StorageConfigException.class)
                .hasMessage("Missing required configuration field: " + fieldName);
    }

    @Test
    void rejectsMissingMaxPayloadSize() {
        String fieldName = "storage.maxPayloadSize";
        StorageConfigDto config = new StorageConfigDto(VALID_MAX_SEGMENT_SIZE, null, VALID_SEGMENT_DIR, VALID_DURABILITY_MODE);

        assertThatThrownBy(() -> mapper.toStorageConfig(config))
                .isInstanceOf(StorageConfigException.class)
                .hasMessage("Missing required configuration field: " + fieldName);
    }

    @Test
    void rejectsMissingSegmentDir() {
        String fieldName = "storage.segmentDir";
        StorageConfigDto config = new StorageConfigDto(VALID_MAX_SEGMENT_SIZE, VALID_MAX_PAYLOAD_SIZE, null, VALID_DURABILITY_MODE);

        assertThatThrownBy(() -> mapper.toStorageConfig(config))
                .isInstanceOf(StorageConfigException.class)
                .hasMessage("Missing required configuration field: " + fieldName);
    }

    @Test
    void rejectsMissingDurabilityMode() {
        String fieldName = "storage.durabilityMode";
        StorageConfigDto config = new StorageConfigDto(VALID_MAX_SEGMENT_SIZE, VALID_MAX_PAYLOAD_SIZE, VALID_SEGMENT_DIR, null);

        assertThatThrownBy(() -> mapper.toStorageConfig(config))
                .isInstanceOf(StorageConfigException.class)
                .hasMessage("Missing required configuration field: " + fieldName);
    }

    @Test
    void wrapsInvalidConfigValuesInStorageConfigException() {
        StorageConfigDto config = new StorageConfigDto(0L, VALID_MAX_PAYLOAD_SIZE, VALID_SEGMENT_DIR, VALID_DURABILITY_MODE);

        assertThatThrownBy(() -> mapper.toStorageConfig(config))
                .isInstanceOf(StorageConfigException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid storage configuration");
    }
}
