package com.ryszardzmija.storage.hash.segment.files;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SegmentFileDiscoverer {
    private final Path segmentDir;

    public SegmentFileDiscoverer(Path segmentDir) {
        this.segmentDir = Objects.requireNonNull(segmentDir);
        if (!Files.isDirectory(this.segmentDir)) {
            throw new SegmentFileException(segmentDir + " is not a directory, expected a directory with segment files");
        }
    }

    /**
     * Returns {@link SegmentLayout} with paths to mutable and immutable segments.
     *
     * @param segmentFileFactory a {@link SegmentFileFactory} object
     * @return a {@link SegmentLayout} with paths to mutable and immutable segment paths
     * @throws SegmentFileDiscoveryException if the directory cannot be read
     * @throws SegmentFileException if no files exist and the initial file cannot be created
     */
    public SegmentLayout getSegmentFiles(SegmentFileFactory segmentFileFactory) {
        List<Path> discoveredFiles = discoverSegmentFiles();

        if (discoveredFiles.isEmpty()) {
            Path mutableSegmentPath = segmentFileFactory.createSegmentFile();
            return new SegmentLayout(Collections.emptyList(), mutableSegmentPath);
        }

        Path mutableSegment = discoveredFiles.getLast();
        discoveredFiles.removeLast();

        return new SegmentLayout(List.copyOf(discoveredFiles), mutableSegment);
    }

    /**
     * Returns all segment files found in the segment directory, ordered from oldest to newest.
     *
     * @return an ordered list of paths to segment files, empty if no segment paths exist
     * @throws SegmentFileDiscoveryException if the segment directory is invalid or cannot be read
     */
    private List<Path> discoverSegmentFiles() {
        try (Stream<Path> files = Files.list(segmentDir)) {
            return files
                    .filter(p -> p.getFileName().toString().matches(FileFormatInfo.getReadFileRegex()))
                    .sorted(Comparator.comparingLong(FileFormatInfo::extractId))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            throw new SegmentFileDiscoveryException("Error discovering segment files in " + segmentDir, e);
        }
    }
}
