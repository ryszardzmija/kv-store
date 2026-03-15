package com.ryszardzmija.storage.hash.segment.rollover;

import com.ryszardzmija.storage.hash.segment.model.MutableSegment;

public class SizeBasedRolloverPolicy implements RolloverPolicy {
    private static final long MAX_SEGMENT_SIZE = 64*1024;  // 64KB

    @Override
    public boolean shouldRollover(MutableSegment segment) {
        return segment.size() >= MAX_SEGMENT_SIZE;
    }
}
