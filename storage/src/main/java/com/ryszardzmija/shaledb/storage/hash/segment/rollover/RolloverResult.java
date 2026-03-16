package com.ryszardzmija.shaledb.storage.hash.segment.rollover;

import com.ryszardzmija.shaledb.storage.hash.segment.model.ImmutableSegment;
import com.ryszardzmija.shaledb.storage.hash.segment.model.MutableSegment;

public record RolloverResult(MutableSegment newMutableSegment, ImmutableSegment newImmutableSegment) {
}
