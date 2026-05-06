package com.ryszardzmija.shaledb.server.config;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class ApplicationConfigLoader {
    private final ObjectMapper mapper;

    public ApplicationConfigLoader(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    public static ApplicationConfigLoader fromYAML() {
        ObjectMapper mapper = YAMLMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                .configure(DeserializationFeature.ACCEPT_FLOAT_AS_INT, false)
                .configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false)
                .build();
        return new ApplicationConfigLoader(mapper);
    }

    public ApplicationConfigDto loadFromFile(Path configFile) {
        checkFileAccessibility(configFile);
        checkFileNotEmpty(configFile);

        ApplicationConfigDto appConfig;

        try {
            appConfig = mapper.readValue(configFile.toFile(), ApplicationConfigDto.class);
        } catch (StreamReadException e) {
            throw new ConfigLoadingException("Configuration file is not valid YAML: " + configFile, e);
        } catch (DatabindException e) {
            throw new ConfigLoadingException("Configuration file has invalid structure or field types: " + configFile, e);
        } catch (IOException e) {
            throw new ConfigLoadingException("Error reading configuration file: " + configFile, e);
        }

        return appConfig;
    }

    private static void checkFileAccessibility(Path configFile) {
        if (configFile == null) {
            throw new ConfigLoadingException("Configuration file must not be null");
        }

        if (!Files.exists(configFile)) {
            throw new ConfigLoadingException("Configuration file does not exist: " + configFile);
        }

        if (!Files.isRegularFile(configFile)) {
            throw new ConfigLoadingException("Configuration path must point to a regular file: " + configFile);
        }

        if (!Files.isReadable(configFile)) {
            throw new ConfigLoadingException("Configuration file is not readable: " + configFile);
        }
    }

    private static void checkFileNotEmpty(Path configFile) {
        try {
            if (Files.size(configFile) == 0) {
                throw new ConfigLoadingException("Configuration file is empty: " + configFile);
            }
        } catch (IOException e) {
            throw new ConfigLoadingException("Error reading configuration file: " + configFile, e);
        }
    }
}
