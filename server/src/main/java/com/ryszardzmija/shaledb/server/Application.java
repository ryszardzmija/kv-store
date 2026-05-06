package com.ryszardzmija.shaledb.server;

import com.ryszardzmija.shaledb.server.config.ApplicationConfigDto;
import com.ryszardzmija.shaledb.server.config.ApplicationConfigLoader;
import com.ryszardzmija.shaledb.storage.KeyValueStore;
import com.ryszardzmija.shaledb.storage.config.StorageConfig;
import com.ryszardzmija.shaledb.storage.config.StorageConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    void main(String[] args) {
        String configFile = "config/application.yaml";
        if (args.length > 0) {
            configFile = args[0];
        }

        ApplicationConfigLoader applicationConfigLoader = ApplicationConfigLoader.fromYAML();
        ApplicationConfigDto applicationConfig = applicationConfigLoader.loadFromFile(Path.of(configFile));

        StorageConfigMapper storageConfigMapper = new StorageConfigMapper();
        StorageConfig storageConfig = storageConfigMapper.toStorageConfig(applicationConfig.storage());

        try {
            Files.createDirectories(storageConfig.segmentDir());
            try (var store = new KeyValueStore(storageConfig)) {
                store.put(getBytes("Greeting"), getBytes("Hello, World!"));
                store.put(getBytes("Answer"), getBytes("42"));

                Optional<byte[]> greetingResult = store.get(getBytes("Greeting"));
                if (greetingResult.isPresent()) {
                    String decodedGreeting = new String(greetingResult.get(), StandardCharsets.UTF_8);
                    System.out.println("Key: Greeting, Value: " + decodedGreeting);
                }
                Optional<byte[]> answerResult = store.get(getBytes("Answer"));
                if (answerResult.isPresent()) {
                    String decodedAnswer = new String(answerResult.get(), StandardCharsets.UTF_8);
                    System.out.println("Key: Answer, Value: " + decodedAnswer);
                }

                store.delete(getBytes("Greeting"));
                greetingResult = store.get(getBytes("Greeting"));
                if (greetingResult.isPresent()) {
                    String decodedGreeting = new String(greetingResult.get(), StandardCharsets.UTF_8);
                    System.out.println("Key: Greeting, Value: " + decodedGreeting);
                } else {
                    System.out.println("Key: 'Greeting' Not Found");
                }

                store.put(getBytes("Greeting"), getBytes("Hello, Universe!"));
                greetingResult = store.get(getBytes("Greeting"));
                if (greetingResult.isPresent()) {
                    String decodedGreeting = new String(greetingResult.get(), StandardCharsets.UTF_8);
                    System.out.println("Key: Greeting, Value: " + decodedGreeting);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to create segment directory {}", storageConfig.segmentDir(), e);
            System.exit(1);
        } catch (RuntimeException e) {
            logger.error("Fatal error during execution", e);
            System.exit(1);
        }
    }

    private byte[] getBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
