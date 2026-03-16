package com.ryszardzmija.shaledb.storage.hash.segment.rollover;

import com.ryszardzmija.shaledb.storage.hash.segment.model.MutableSegment;

public interface RolloverPolicy {
    boolean shouldRollover(MutableSegment segment);
}
