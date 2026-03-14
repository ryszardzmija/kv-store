package com.ryszardzmija.segment.model;

import com.ryszardzmija.format.RecordWriteResult;
import com.ryszardzmija.format.RecordWriter;
import com.ryszardzmija.format.Record;
import com.ryszardzmija.index.ByteKey;
import com.ryszardzmija.index.Index;

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
