package com.ryszardzmija.shaledb.storage.hash.segment.rollover;

import com.ryszardzmija.shaledb.storage.durability.DurabilityConfig;
import com.ryszardzmija.shaledb.storage.durability.DurabilityException;
import com.ryszardzmija.shaledb.storage.durability.FileSystemSync;
import com.ryszardzmija.shaledb.storage.hash.segment.files.SegmentFileException;
import com.ryszardzmija.shaledb.storage.hash.segment.files.SegmentFileFactory;
import com.ryszardzmija.shaledb.storage.hash.segment.model.ImmutableSegment;
import com.ryszardzmija.shaledb.storage.hash.segment.model.MutableSegment;
import com.ryszardzmija.shaledb.storage.hash.segment.model.SegmentConfig;
import com.ryszardzmija.shaledb.storage.hash.segment.model.SegmentIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class RolloverHandler {
    private static final Logger logger = LoggerFactory.getLogger(RolloverHandler.class);

    private final SegmentFileFactory segmentFileFactory;
    private final FileSystemSync fileSystemSync;
    private final SegmentConfig segmentConfig;
    private final DurabilityConfig durabilityConfig;

    public RolloverHandler(SegmentFileFactory segmentFileFactory, FileSystemSync fileSystemSync, SegmentConfig segmentConfig, DurabilityConfig durabilityConfig) {
        this.segmentFileFactory = Objects.requireNonNull(segmentFileFactory);
        this.fileSystemSync = Objects.requireNonNull(fileSystemSync);
        this.segmentConfig = Objects.requireNonNull(segmentConfig);
        this.durabilityConfig = Objects.requireNonNull(durabilityConfig);
    }

    public RolloverResult rollOver(MutableSegment segment) {
        Path newSegmentPath = null;
        try {
            newSegmentPath = segmentFileFactory.createSegmentFile();
            MutableSegment newMutableSegment = new MutableSegment(newSegmentPath, segmentConfig, durabilityConfig);

            ImmutableSegment newImmutableSegment;
            try {
                segment.sync();
                newImmutableSegment = new ImmutableSegment(segment.path(), segmentConfig);
                segment.close();
            } catch (SegmentIOException e) {
                newMutableSegment.close();
                throw e;
            }

            return new RolloverResult(newMutableSegment, newImmutableSegment);
        } catch (SegmentFileException | SegmentIOException e) {
            if (newSegmentPath != null) {
                try {
                    Files.delete(newSegmentPath);
                    fileSystemSync.forceDirectory(segmentFileFactory.getSegmentDir());
                } catch (IOException de) {
                    logger.warn("Failed to delete orphaned segment file: {}", newSegmentPath, de);
                } catch (DurabilityException de) {
                    logger.warn("Failed to persist orphaned file deletion to storage: {}", segmentFileFactory.getSegmentDir(), de);
                }
            }
            throw new RolloverException("Failed to roll over the segment " + segment.path(), e);
        }
    }
}
