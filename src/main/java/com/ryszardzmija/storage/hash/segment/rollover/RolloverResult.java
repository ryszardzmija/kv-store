package com.ryszardzmija.storage.hash.segment.rollover;

import com.ryszardzmija.storage.hash.segment.model.ImmutableSegment;
import com.ryszardzmija.storage.hash.segment.model.MutableSegment;

public record RolloverResult(MutableSegment newMutableSegment, ImmutableSegment newImmutableSegment) {
}
