package com.ryszardzmija.shaledb.storage.hash.segment.model;

import com.ryszardzmija.shaledb.storage.serialization.io.WriteRequest;
import com.ryszardzmija.shaledb.storage.serialization.io.WriteResult;
import com.ryszardzmija.shaledb.storage.serialization.io.RecordWriter;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordPayload;
import com.ryszardzmija.shaledb.storage.hash.index.ByteKey;
import com.ryszardzmija.shaledb.storage.hash.index.Index;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordType;

import java.util.Objects;

public class SegmentWriter {
    private final RecordWriter recordWriter;
    private final Index index;

    public SegmentWriter(RecordWriter recordWriter, Index index) {
        this.recordWriter = Objects.requireNonNull(recordWriter);
        this.index = Objects.requireNonNull(index);
    }

    public void put(ByteKey key, byte[] value) {
        RecordPayload recordPayload = new RecordPayload(key.getData(), value);
        WriteRequest writeRequest = new WriteRequest(recordPayload, RecordType.NORMAL);
        WriteResult writeResult = recordWriter.write(writeRequest);
        index.markPresent(key, writeResult.writeOffset());
    }

    public void delete(ByteKey key) {
        WriteRequest writeRequest = new WriteRequest(RecordPayload.forTombstone(key.getData()), RecordType.TOMBSTONE);
        recordWriter.write(writeRequest);
        index.markDeleted(key);
    }
}
