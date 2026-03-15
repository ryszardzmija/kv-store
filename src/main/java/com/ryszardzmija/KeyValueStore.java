package com.ryszardzmija;

import com.ryszardzmija.storage.hash.HashIndexEngine;
import com.ryszardzmija.storage.StorageEngine;
import com.ryszardzmija.storage.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public class KeyValueStore implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueStore.class);

    private final StorageEngine storageEngine;

    public KeyValueStore(Path segmentDir) {
        Objects.requireNonNull(segmentDir);

        try {
            this.storageEngine = new HashIndexEngine(segmentDir);
        } catch (StorageEngineException e) {
            logger.error("Key-value store startup failed", e);
            throw e;
        }
    }

    public void put(byte[] key, byte[] value) {
        try {
            storageEngine.put(key, value);
        } catch (StorageEngineException e) {
            logger.error("Failed to store key-value pair", e);
            throw e;
        }
    }

    public Optional<byte[]> get(byte[] key) {
        try {
            return storageEngine.get(key);
        } catch (StorageEngineException e) {
            logger.error("Failed to retrieve key-value pair", e);
            throw e;
        }
    }

    @Override
    public void close() {
        storageEngine.close();
    }

}
