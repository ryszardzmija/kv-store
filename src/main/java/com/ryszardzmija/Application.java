package com.ryszardzmija;

import com.ryszardzmija.storage.config.StorageConfig;
import com.ryszardzmija.storage.config.StorageConfigHolder;
import com.ryszardzmija.storage.config.StorageConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    void main() {
        StorageConfig storageConfig = StorageConfigLoader.loadFromFile(Path.of("config/application.yaml"));
        StorageConfigHolder.initialize(storageConfig);

        Path segmentDir = Path.of("data/segments");
        try {
            Files.createDirectories(segmentDir);
            try (var store = new KeyValueStore(segmentDir)) {
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

                store.put(getBytes("Greeting"), getBytes("Hello, Universe!"));
                greetingResult = store.get(getBytes("Greeting"));
                if (greetingResult.isPresent()) {
                    String decodedGreeting = new String(greetingResult.get(), StandardCharsets.UTF_8);
                    System.out.println("Key: Greeting, Value: " + decodedGreeting);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to create segment directory {}", segmentDir, e);
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
