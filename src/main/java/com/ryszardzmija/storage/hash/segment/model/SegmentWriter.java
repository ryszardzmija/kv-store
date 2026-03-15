package com.ryszardzmija.storage.hash.segment.model;

import com.ryszardzmija.storage.format.RecordWriteResult;
import com.ryszardzmija.storage.format.RecordWriter;
import com.ryszardzmija.storage.format.Record;
import com.ryszardzmija.storage.hash.index.ByteKey;
import com.ryszardzmija.storage.hash.index.Index;

import java.util.Objects;

public class SegmentWriter {
    private final RecordWriter recordWriter;
    private final Index index;

    public SegmentWriter(RecordWriter recordWriter, Index index) {
        this.recordWriter = Objects.requireNonNull(recordWriter);
        this.index = Objects.requireNonNull(index);
    }

    public void put(ByteKey key, byte[] value) {
        Record record = new Record(key.getData(), value);
        RecordWriteResult writeResult = recordWriter.write(record);
        index.putKeyOffset(key, writeResult.writeOffset());
    }
}
