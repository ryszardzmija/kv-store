package com.ryszardzmija.storage.hash.segment.model;

import com.ryszardzmija.storage.format.RecordReadResult;
import com.ryszardzmija.storage.format.RecordReader;
import com.ryszardzmija.storage.hash.index.ByteKey;
import com.ryszardzmija.storage.hash.index.Index;

import java.util.Objects;
import java.util.Optional;

public class SegmentReader {
    private final RecordReader recordReader;
    private final Index index;

    public SegmentReader(RecordReader recordReader, Index index) {
        this.recordReader = Objects.requireNonNull(recordReader);
        this.index = Objects.requireNonNull(index);
    }

    public Optional<byte[]> get(ByteKey key) {
        Optional<Long> offset = index.getKeyOffset(key);

        if (offset.isEmpty()) {
            return Optional.empty();
        }

        RecordReadResult readResult = recordReader.read(offset.get());
        return Optional.of(readResult.record().value());
    }
}
