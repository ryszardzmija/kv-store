package com.ryszardzmija.storage.hash.segment.rollover;

import com.ryszardzmija.storage.hash.segment.model.MutableSegment;

public interface RolloverPolicy {
    boolean shouldRollover(MutableSegment segment);
}
