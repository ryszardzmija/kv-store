package com.ryszardzmija.segment;

import com.ryszardzmija.index.ByteKey;
import com.ryszardzmija.segment.files.*;
import com.ryszardzmija.segment.loader.LoadedSegments;
import com.ryszardzmija.segment.loader.SegmentLoader;
import com.ryszardzmija.segment.loader.SegmentLoadingException;
import com.ryszardzmija.segment.model.ImmutableSegment;
import com.ryszardzmija.segment.model.MutableSegment;
import com.ryszardzmija.segment.model.SegmentIOException;
import com.ryszardzmija.segment.rollover.*;

import java.nio.file.Path;
import java.util.*;

public class SegmentManager implements AutoCloseable {

    private final RolloverPolicy rolloverPolicy;
    private final RolloverHandler rolloverHandler;

    private final Deque<ImmutableSegment> immutableSegments;
    private MutableSegment mutableSegment;

    public SegmentManager(Path segmentDir) {
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
            throw new SegmentManagerException("Failed to initialize segment manager", e);
        }
    }

    public void put(byte[] key, byte[] value) {
        try {
            mutableSegment.put(new ByteKey(key), value);

            handleRollover();
        } catch (SegmentIOException e) {
            throw new SegmentManagerException("Failed to write segment", e);
        } catch (RolloverException e) {
            throw new SegmentManagerException("Failed to roll over to new segment", e);
        }
    }

    public Optional<byte[]> get(byte[] key) {
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
            throw new SegmentManagerException("Failed to read segment", e);
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
