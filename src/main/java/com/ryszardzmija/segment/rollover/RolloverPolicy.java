package com.ryszardzmija.segment.rollover;

import com.ryszardzmija.segment.model.MutableSegment;

public interface RolloverPolicy {
    boolean shouldRollover(MutableSegment segment);
}
