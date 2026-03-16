package com.ryszardzmija.shaledb.storage.hash.segment.loader;

import com.ryszardzmija.shaledb.storage.hash.segment.model.ImmutableSegment;
import com.ryszardzmija.shaledb.storage.hash.segment.model.MutableSegment;

import java.util.Deque;

public record LoadedSegments(Deque<ImmutableSegment> immutableSegments, MutableSegment mutableSegment) {
}
