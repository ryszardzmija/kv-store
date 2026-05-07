package com.ryszardzmija.shaledb.storage.hash.segment.model;

import com.ryszardzmija.shaledb.storage.durability.DurabilityMode;
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
    private final DurabilityMode durabilityMode;

    public SegmentWriter(RecordWriter recordWriter, Index index, DurabilityMode durabilityMode) {
        this.recordWriter = Objects.requireNonNull(recordWriter);
        this.index = Objects.requireNonNull(index);
        this.durabilityMode = Objects.requireNonNull(durabilityMode);
    }

    public void put(ByteKey key, byte[] value) {
        // Write record bytes
        RecordPayload recordPayload = new RecordPayload(key.getData(), value);
        WriteRequest writeRequest = new WriteRequest(recordPayload, RecordType.NORMAL);
        WriteResult writeResult = recordWriter.write(writeRequest);

        // Apply durability strategy
        // NOTE: SYNC_EACH_WRITE is a correctness first mode and hurts
        // throughput significantly. For a production workload another strategy
        // needs to be devised which batches multiple writes before fsync and
        // implements a crash recovery strategy.
        if (durabilityMode == DurabilityMode.SYNC_EACH_WRITE) {
            recordWriter.flushToStorage();
        }

        // Update in-memory index
        index.markPresent(key, writeResult.writeOffset());
    }

    public void delete(ByteKey key) {
        WriteRequest writeRequest = new WriteRequest(RecordPayload.forTombstone(key.getData()), RecordType.TOMBSTONE);
        recordWriter.write(writeRequest);

        if (durabilityMode == DurabilityMode.SYNC_EACH_WRITE) {
            recordWriter.flushToStorage();
        }

        index.markDeleted(key);
    }
}
