package com.ryszardzmija.shaledb.storage.hash.segment.loader;

import com.ryszardzmija.shaledb.storage.hash.segment.model.ImmutableSegment;
import com.ryszardzmija.shaledb.storage.hash.segment.model.MutableSegment;
import com.ryszardzmija.shaledb.storage.hash.segment.files.SegmentLayout;
import com.ryszardzmija.shaledb.storage.hash.segment.model.SegmentIOException;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class SegmentLoader {
    public LoadedSegments loadSegments(SegmentLayout segmentLayout) {
        List<Path> immutableSegmentPaths = segmentLayout.immutableSegmentPaths();
        Deque<ImmutableSegment> immutableSegments = new ArrayDeque<>();

        try {
            // Iterate from the newest segment file to the oldest.
            for (int i = immutableSegmentPaths.size() - 1; i >= 0; i--) {
                immutableSegments.addLast(new ImmutableSegment(immutableSegmentPaths.get(i)));
            }

            Path mutableSegmentPath = segmentLayout.mutableSegmentPath();
            MutableSegment mutableSegment = new MutableSegment(mutableSegmentPath);

            return new LoadedSegments(immutableSegments, mutableSegment);
        } catch (SegmentIOException e) {
            closeAll(immutableSegments);
            throw new SegmentLoadingException("Failed to load segments", e);
        }
    }

    private void closeAll(Deque<ImmutableSegment> segments) {
        for (ImmutableSegment segment : segments) {
            segment.close();
        }
    }
}
