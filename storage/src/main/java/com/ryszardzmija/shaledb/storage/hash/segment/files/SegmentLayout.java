package com.ryszardzmija.shaledb.storage.hash.segment.files;

import java.nio.file.Path;
import java.util.List;

/**
 * Represents file paths of segment files discovered on disk.
 * @param immutableSegmentPaths paths to immutable segments ordered from oldest to newest
 * @param mutableSegmentPath path to the current mutable segment
 */
public record SegmentLayout(List<Path> immutableSegmentPaths, Path mutableSegmentPath) {
}
