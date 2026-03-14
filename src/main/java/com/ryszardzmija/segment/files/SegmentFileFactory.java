package com.ryszardzmija.segment.files;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.stream.Stream;

public class SegmentFileFactory {
    private final Path segmentDir;
    private long lastId;

    public SegmentFileFactory(Path segmentDir) {
        this.segmentDir = Objects.requireNonNull(segmentDir);
        if (!Files.isDirectory(this.segmentDir)) {
            throw new SegmentFileException(segmentDir + " is not a directory, expected a directory with segment files");
        }
        this.lastId = resolveLastId().orElse(FileFormatInfo.getFirstId() - 1);
    }

    /**
     * Creates a new segment file and returns a {@link Path} to it.
     *
     * @return a {@link Path} to the newly created segment file
     * @throws SegmentFileException if the segment file cannot be created
     */
    public Path createSegmentFile() {
        lastId++;
        Path segmentPath = segmentDir.resolve(FileFormatInfo.getWriteFileFormat().formatted(lastId));
        try {
            return Files.createFile(segmentPath);
        } catch (IOException e) {
            throw new SegmentFileException("Failed to create a segment file " + segmentPath, e);
        }
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
