package com.ryszardzmija.shaledb.storage.hash.segment.files;

import com.ryszardzmija.shaledb.storage.durability.DurabilityException;
import com.ryszardzmija.shaledb.storage.durability.FileSystemSync;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.stream.Stream;

public class SegmentFileFactory {
    private final Path segmentDir;
    private final FileSystemSync fileSystemSync;

    // segmentIds are monotonically increasing, not necessarily contiguous
    private long lastId;

    public SegmentFileFactory(Path segmentDir, FileSystemSync fileSystemSync) {
        this.segmentDir = Objects.requireNonNull(segmentDir);
        if (!Files.isDirectory(this.segmentDir)) {
            throw new SegmentFileException(segmentDir + " is not a directory, expected a directory with segment files");
        }
        this.fileSystemSync = Objects.requireNonNull(fileSystemSync);
        this.lastId = resolveLastId().orElse(FileFormatInfo.getFirstId() - 1);
    }

    /**
     * Creates a new segment file, persists it to storage,
     * and returns a {@link Path} to the new segment file.
     *
     * @return a {@link Path} to the newly created segment file
     * @throws SegmentFileException if the segment file cannot be created or flushed to persistent storage
     */
    public Path createSegmentFile() {
        lastId++;
        Path segmentPath = segmentDir.resolve(FileFormatInfo.getWriteFileFormat().formatted(lastId));

        Path newSegmentFile;
        try {
            newSegmentFile = Files.createFile(segmentPath);
            fileSystemSync.forceDirectory(segmentDir);
            return newSegmentFile;
        } catch (IOException e) {
            throw new SegmentFileException("Failed to create a segment file " + segmentPath, e);
        } catch (DurabilityException e) {
            try {
                Files.deleteIfExists(segmentPath);
                fileSystemSync.forceDirectory(segmentDir);
            } catch (IOException | DurabilityException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw new SegmentFileException("Failed to flush segment file creation: " + segmentPath, e);
        }
    }

    public Path getSegmentDir() {
        return segmentDir;
    }

    private OptionalLong resolveLastId() {
        try (Stream<Path> files = Files.list(segmentDir)) {
            return files
                    .filter(p -> p.getFileName().toString().matches(FileFormatInfo.getReadFileRegex()))
                    .mapToLong(FileFormatInfo::extractId)
                    .max();
        } catch (IOException e) {
            throw new SegmentFileException("Failed to resolve the last ID of a segment file in " + segmentDir, e);
        }
    }
}
