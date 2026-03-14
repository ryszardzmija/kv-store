package com.ryszardzmija.segment.rollover;

import com.ryszardzmija.segment.model.MutableSegment;

public class SizeBasedRolloverPolicy implements RolloverPolicy {
    private static final long MAX_SEGMENT_SIZE = 64*1024;  // 64KB

    @Override
    public boolean shouldRollover(MutableSegment segment) {
        return segment.size() >= MAX_SEGMENT_SIZE;
    }
}
