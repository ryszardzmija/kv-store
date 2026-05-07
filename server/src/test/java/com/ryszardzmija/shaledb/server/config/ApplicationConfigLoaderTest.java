package com.ryszardzmija.shaledb.server.config;

import com.ryszardzmija.shaledb.storage.durability.DurabilityMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApplicationConfigLoaderTest {
    @TempDir
    Path tempDir;

    ApplicationConfigLoader loader;

    private Path writeConfigFile(String content) throws IOException {
        Path configFile = tempDir.resolve("application.yaml");
        Files.writeString(configFile, content);

        return configFile;
    }

    @BeforeEach
    void setUp() {
        loader = ApplicationConfigLoader.fromYAML();
    }

    @Test
    void rejectsNullConfigFilePath() {
        assertThatThrownBy(() -> loader.loadFromFile(null))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessage("Configuration file must not be null");
    }

    @Test
    void rejectsMissingConfigFile() {
        Path missingFile = Path.of("invalid/path");

        assertThatThrownBy(() -> loader.loadFromFile(missingFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessage("Configuration file does not exist: " + missingFile);
    }

    @Test
    void rejectsDirectoryInsteadOfFile() {
        assertThatThrownBy(() -> loader.loadFromFile(tempDir))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessage("Configuration path must point to a regular file: " + tempDir);
    }

    @Test
    void rejectsEmptyConfigFile() throws IOException {
        Path configFile = writeConfigFile("");

        assertThatThrownBy(() -> loader.loadFromFile(configFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessage("Configuration file is empty: " + configFile);
    }

    @Test
    void rejectsMalformedYaml() throws IOException {
        Path configFile = writeConfigFile("""
                storage
                  maxSegmentSize: 65536
                  maxPayloadSize: 16384
                  segmentDir: data/segments
                  durabilityMode: SYNC_EACH_WRITE
                """);

        assertThatThrownBy(() -> loader.loadFromFile(configFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessageContaining("Configuration file");
    }

    @Test
    void rejectsUnknownNestedProperty() throws IOException {
        Path configFile = writeConfigFile("""
                storage:
                  maxSegmentSize: 65536
                  maxPayloadSize: 16384
                  segmentDir: data/segments
                  durabilityMode: SYNC_EACH_WRITE
                  unknownProperty: 10
                """);

        assertThatThrownBy(() -> loader.loadFromFile(configFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessageContaining("Configuration file has invalid structure or field types: " + configFile);
    }

    @Test
    void rejectsUnknownTopLevelProperty() throws IOException {
        Path configFile = writeConfigFile("""
                unknownProperty:
                  maxSegmentSize: 65536
                  maxPayloadSize: 16384
                  segmentDir: data/segments
                  durabilityMode: SYNC_EACH_WRITE
                """);

        assertThatThrownBy(() -> loader.loadFromFile(configFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessageContaining("Configuration file has invalid structure or field types: " + configFile);
    }

    @Test
    void rejectsValueRequiringFloatCoercion() throws IOException {
        Path configFile = writeConfigFile("""
                storage:
                  maxSegmentSize: 65536
                  maxPayloadSize: 3.1415
                  segmentDir: data/segments
                  durabilityMode: SYNC_EACH_WRITE
                """);

        assertThatThrownBy(() -> loader.loadFromFile(configFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessageContaining("Configuration file has invalid structure or field types: " + configFile);
    }

    @Test
    void rejectsValueRequiringCoercionOfScalars() throws IOException {
        Path configFile = writeConfigFile("""
                storage:
                  maxSegmentSize: 65536
                  maxPayloadSize: "16384"
                  segmentDir: data/segments
                  durabilityMode: SYNC_EACH_WRITE
                """);

        assertThatThrownBy(() -> loader.loadFromFile(configFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessageContaining("Configuration file has invalid structure or field types: " + configFile);
    }

    @Test
    void rejectsInvalidEnumValue() throws IOException {
        Path configFile = writeConfigFile("""
                storage:
                  maxSegmentSize: 65536
                  maxPayloadSize: 16384
                  segmentDir: data/segments
                  durabilityMode: sync_each_write
                """);

        assertThatThrownBy(() -> loader.loadFromFile(configFile))
                .isInstanceOf(ConfigLoadingException.class)
                .hasMessageContaining("Configuration file has invalid structure or field types: " + configFile);
    }

    @Test
    void loadsValidYamlConfig() throws IOException {
        Path configFile = writeConfigFile("""
                storage:
                  maxSegmentSize: 65536
                  maxPayloadSize: 16384
                  segmentDir: data/segments
                  durabilityMode: SYNC_EACH_WRITE
                """);

        ApplicationConfigDto config = loader.loadFromFile(configFile);

        assertThat(config.storage().maxSegmentSize()).isEqualTo(65536L);
        assertThat(config.storage().maxPayloadSize()).isEqualTo(16384L);
        assertThat(config.storage().segmentDir()).isEqualTo("data/segments");
        assertThat(config.storage().durabilityMode()).isEqualTo(DurabilityMode.SYNC_EACH_WRITE);
    }
}
