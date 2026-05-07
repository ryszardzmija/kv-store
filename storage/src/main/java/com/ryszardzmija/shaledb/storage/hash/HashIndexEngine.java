package com.ryszardzmija.shaledb.storage.hash;

import com.ryszardzmija.shaledb.storage.StorageEngine;
import com.ryszardzmija.shaledb.storage.StorageEngineException;
import com.ryszardzmija.shaledb.storage.config.StorageConfig;
import com.ryszardzmija.shaledb.storage.durability.DurabilityConfig;
import com.ryszardzmija.shaledb.storage.durability.FileSystemSync;
import com.ryszardzmija.shaledb.storage.hash.index.ByteKey;
import com.ryszardzmija.shaledb.storage.hash.segment.files.*;
import com.ryszardzmija.shaledb.storage.hash.segment.loader.LoadedSegments;
import com.ryszardzmija.shaledb.storage.hash.segment.loader.SegmentLoader;
import com.ryszardzmija.shaledb.storage.hash.segment.loader.SegmentLoadingException;
import com.ryszardzmija.shaledb.storage.hash.segment.model.*;
import com.ryszardzmija.shaledb.storage.hash.segment.rollover.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HashIndexEngine implements StorageEngine {
    private static final Logger logger = LoggerFactory.getLogger(HashIndexEngine.class);

    private final RolloverPolicy rolloverPolicy;
    private final RolloverHandler rolloverHandler;

    private final Deque<ImmutableSegment> immutableSegments;
    private MutableSegment mutableSegment;

    public HashIndexEngine(StorageConfig storageConfig) {
        Objects.requireNonNull(storageConfig);

        try {
            SegmentConfig segmentConfig = SegmentConfig.from(storageConfig);
            DurabilityConfig durabilityConfig = DurabilityConfig.from(storageConfig);

            SegmentFileFactory segmentFileFactory = new SegmentFileFactory(storageConfig.segmentDir(), new FileSystemSync());
            this.rolloverPolicy = new SizeBasedRolloverPolicy(segmentConfig.maxSegmentSize());
            this.rolloverHandler = new RolloverHandler(segmentFileFactory, new FileSystemSync(), segmentConfig, durabilityConfig);
            SegmentFileDiscoverer segmentFileDiscoverer = new SegmentFileDiscoverer(storageConfig.segmentDir());
            SegmentLoader segmentLoader = new SegmentLoader(segmentConfig, durabilityConfig);

            SegmentLayout segmentLayout = segmentFileDiscoverer.getSegmentFiles(segmentFileFactory);
            LoadedSegments loadedSegments = segmentLoader.loadSegments(segmentLayout);
            this.immutableSegments = loadedSegments.immutableSegments();
            this.mutableSegment = loadedSegments.mutableSegment();
        } catch (SegmentFileException | SegmentFileDiscoveryException | SegmentLoadingException e) {
            throw new StorageEngineException("Failed to initialize hash index storage engine", e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }

        try {
            mutableSegment.put(new ByteKey(key), value);
        } catch (SegmentIOException e) {
            throw new StorageEngineException("Failed to write segment", e);
        }

        try {
            handleRollover();
        } catch (RolloverException e) {
            // TODO: Decide whether to implement retry logic or wait for the next
            // TODO: write for a retry. If the rollover keeps failing we should
            // TODO: bound the size of the segment and probably refuse writes after
            // TODO: some threshold is reached.
            logger.warn("Rollover failed after successful write", e);
        }
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }

        ByteKey byteKey = new ByteKey(key);

        try {
            LookupResult mutableResult = mutableSegment.get(byteKey);
            switch (mutableResult) {
                case LookupResult.Deleted _ -> { return Optional.empty(); }
                case LookupResult.Present mappedValue -> { return Optional.of(mappedValue.value()); }
                case LookupResult.NotPresent _ -> {  /* continue */ }
            }

            for (ImmutableSegment segment : immutableSegments) {
                LookupResult immutableResult = segment.get(byteKey);
                switch (immutableResult) {
                    case LookupResult.Deleted _ -> { return Optional.empty(); }
                    case LookupResult.Present mappedValue -> { return Optional.of(mappedValue.value()); }
                    case LookupResult.NotPresent _ -> {  /* continue */ }
                }
            }

            return Optional.empty();
        } catch (SegmentIOException e) {
            throw new StorageEngineException("Failed to read segment", e);
        }
    }

    /**
     * Deletes the key-value pair associated with the key.
     *
     * <p>Note that each deletion writes a tombstone record for the key, which means that
     * even though repeated deletions on the same key do not change the state of the store,
     * they are still accumulated on the disk.
     *
     * @param key a non-empty array of bytes representing the key
     * @throws IllegalArgumentException if the key is null or empty
     * @throws StorageEngineException if the key-value pair associated with the key fails to be deleted
     */
    @Override
    public void delete(byte[] key) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }

        try {
            mutableSegment.delete(new ByteKey(key));
        } catch (SegmentIOException e) {
            throw new StorageEngineException("Failed to write segment", e);
        }

        try {
            handleRollover();
        } catch (RolloverException e) {
            logger.warn("Rollover failed after successful delete", e);
        }
    }

    @Override
    public void close() {
        StorageEngineException exception = null;
        try {
            mutableSegment.sync();
        } catch (SegmentIOException e) {
            exception = new StorageEngineException("Failed to flush writeable segment to persistent storage", e);
        }

        mutableSegment.close();

        for (ImmutableSegment segment : immutableSegments) {
            segment.close();
        }

        if (exception != null) {
            throw exception;
        }
    }

    private void handleRollover() {
        if (rolloverPolicy.shouldRollover(mutableSegment)) {
            RolloverResult rolloverResult = rolloverHandler.rollOver(mutableSegment);
            mutableSegment = rolloverResult.newMutableSegment();
            immutableSegments.addFirst(rolloverResult.newImmutableSegment());
        }
    }
}
