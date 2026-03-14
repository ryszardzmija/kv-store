package com.ryszardzmija;

import com.ryszardzmija.segment.SegmentManager;
import com.ryszardzmija.segment.SegmentManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public class KeyValueStore implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueStore.class);

    private final SegmentManager segmentManager;

    public KeyValueStore(Path segmentDir) {
        Objects.requireNonNull(segmentDir);

        try {
            this.segmentManager = new SegmentManager(segmentDir);
        } catch (SegmentManagerException e) {
            logger.error("Key-value store startup failed", e);
            throw e;
        }
    }

    public void put(byte[] key, byte[] value) {
        try {
            segmentManager.put(key, value);
        } catch (SegmentManagerException e) {
            logger.error("Failed to store key-value pair", e);
            throw e;
        }
    }

    public Optional<byte[]> get(byte[] key) {
        try {
            return segmentManager.get(key);
        } catch (SegmentManagerException e) {
            logger.error("Failed to retrieve key-value pair", e);
            throw e;
        }
    }

    @Override
    public void close() {
        segmentManager.close();
    }

}
