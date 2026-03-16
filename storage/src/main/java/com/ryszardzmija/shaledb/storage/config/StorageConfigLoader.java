package com.ryszardzmija.shaledb.storage.config;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.ryszardzmija.shaledb.storage.config.dto.ApplicationConfigDto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class StorageConfigLoader {
    public static StorageConfig loadFromFile(Path configFile) {
        if (!Files.exists(configFile)) {
            throw new ConfigLoadingException("Configuration file not found: " + configFile);
        }

        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            ApplicationConfigDto appConfig = mapper.readValue(configFile.toFile(), ApplicationConfigDto.class);

            if (appConfig.storage == null) {
                throw new ConfigLoadingException("Missing required 'storage' section in config file: " + configFile);
            }

            return appConfig.storage.toStorageConfig();
        } catch (StreamReadException e) {
            throw new ConfigLoadingException("Failed to parse the config file: " + configFile, e);
        } catch (DatabindException e) {
            throw new ConfigLoadingException("Failed to deserialize the config file: " + configFile, e);
        } catch (IOException e) {
            throw new ConfigLoadingException("Failed to read the config file: " + configFile, e);
        }
    }
}
