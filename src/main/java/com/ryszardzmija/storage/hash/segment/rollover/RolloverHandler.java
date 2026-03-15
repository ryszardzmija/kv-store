package com.ryszardzmija.storage.hash.segment.rollover;

import com.ryszardzmija.storage.hash.segment.files.SegmentFileException;
import com.ryszardzmija.storage.hash.segment.files.SegmentFileFactory;
import com.ryszardzmija.storage.hash.segment.model.ImmutableSegment;
import com.ryszardzmija.storage.hash.segment.model.MutableSegment;
import com.ryszardzmija.storage.hash.segment.model.SegmentIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class RolloverHandler {
    private static final Logger logger = LoggerFactory.getLogger(RolloverHandler.class);

    private final SegmentFileFactory segmentFileFactory;

    public RolloverHandler(SegmentFileFactory segmentFileFactory) {
        this.segmentFileFactory = Objects.requireNonNull(segmentFileFactory);
    }

    public RolloverResult rollOver(MutableSegment segment) {
        Path newSegmentPath = null;
        try {
            newSegmentPath = segmentFileFactory.createSegmentFile();
            MutableSegment newMutableSegment = new MutableSegment(newSegmentPath);

            try {
                ImmutableSegment newImmutableSegment = new ImmutableSegment(segment.path());
                segment.close();
                return new RolloverResult(newMutableSegment, newImmutableSegment);
            } catch (SegmentIOException e) {
                newMutableSegment.close();
                throw e;
            }
        } catch (SegmentFileException | SegmentIOException e) {
            if (newSegmentPath != null) {
                try {
                    Files.delete(newSegmentPath);
                } catch (IOException de) {
                    logger.warn("Failed to delete orphaned segment file: {}", newSegmentPath, de);
                }
            }
            throw new RolloverException("Failed to roll over the segment " + segment.path(), e);
        }
    }
}
