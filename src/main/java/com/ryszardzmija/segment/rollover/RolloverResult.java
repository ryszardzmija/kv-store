package com.ryszardzmija.segment.rollover;

import com.ryszardzmija.segment.model.ImmutableSegment;
import com.ryszardzmija.segment.model.MutableSegment;

public record RolloverResult(MutableSegment newMutableSegment, ImmutableSegment newImmutableSegment) {
}
