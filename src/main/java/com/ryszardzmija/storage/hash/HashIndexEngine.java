package com.ryszardzmija.storage.hash;

import com.ryszardzmija.storage.StorageEngine;
import com.ryszardzmija.storage.StorageEngineException;
import com.ryszardzmija.storage.hash.index.ByteKey;
import com.ryszardzmija.storage.hash.segment.files.*;
import com.ryszardzmija.storage.hash.segment.loader.LoadedSegments;
import com.ryszardzmija.storage.hash.segment.loader.SegmentLoader;
import com.ryszardzmija.storage.hash.segment.loader.SegmentLoadingException;
import com.ryszardzmija.storage.hash.segment.model.ImmutableSegment;
import com.ryszardzmija.storage.hash.segment.model.MutableSegment;
import com.ryszardzmija.storage.hash.segment.model.SegmentIOException;
import com.ryszardzmija.storage.hash.segment.rollover.*;

import java.nio.file.Path;
import java.util.*;

public class HashIndexEngine implements StorageEngine {
    private final RolloverPolicy rolloverPolicy;
    private final RolloverHandler rolloverHandler;

    private final Deque<ImmutableSegment> immutableSegments;
    private MutableSegment mutableSegment;

    public HashIndexEngine(Path segmentDir) {
        Objects.requireNonNull(segmentDir);

        try {
            SegmentFileFactory segmentFileFactory = new SegmentFileFactory(segmentDir);
            this.rolloverPolicy = new SizeBasedRolloverPolicy();
            this.rolloverHandler = new RolloverHandler(segmentFileFactory);
            SegmentFileDiscoverer segmentFileDiscoverer = new SegmentFileDiscoverer(segmentDir);
            SegmentLoader segmentLoader = new SegmentLoader();

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

            handleRollover();
        } catch (SegmentIOException e) {
            throw new StorageEngineException("Failed to write segment", e);
        } catch (RolloverException e) {
            throw new StorageEngineException("Failed to roll over to new segment", e);
        }
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }

        ByteKey byteKey = new ByteKey(key);

        try {
            Optional<byte[]> result = mutableSegment.get(byteKey);
            if (result.isPresent()) {
                return result;
            }

            for (ImmutableSegment segment : immutableSegments) {
                result = segment.get(byteKey);
                if (result.isPresent()) {
                    return result;
                }
            }

            return Optional.empty();
        } catch (SegmentIOException e) {
            throw new StorageEngineException("Failed to read segment", e);
        }
    }

    @Override
    public void close() {
        mutableSegment.close();

        for (ImmutableSegment segment : immutableSegments) {
            segment.close();
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
