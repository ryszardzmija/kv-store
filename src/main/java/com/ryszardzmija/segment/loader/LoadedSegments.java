package com.ryszardzmija.segment.loader;

import com.ryszardzmija.segment.model.ImmutableSegment;
import com.ryszardzmija.segment.model.MutableSegment;

import java.util.Deque;

public record LoadedSegments(Deque<ImmutableSegment> immutableSegments, MutableSegment mutableSegment) {
}
