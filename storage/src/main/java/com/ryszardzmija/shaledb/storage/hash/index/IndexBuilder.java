package com.ryszardzmija.shaledb.storage.hash.index;

import com.ryszardzmija.shaledb.storage.serialization.io.ReadRequest;
import com.ryszardzmija.shaledb.storage.serialization.io.ReadResult;
import com.ryszardzmija.shaledb.storage.serialization.io.RecordIOException;
import com.ryszardzmija.shaledb.storage.serialization.io.RecordReader;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordType;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class IndexBuilder {
    private final FileChannel readChannel;
    private final RecordReader recordReader;

    public IndexBuilder(FileChannel readChannel) {
        this.readChannel = Objects.requireNonNull(readChannel);
        this.recordReader = new RecordReader(readChannel);
    }

    public Index build() {
        try {
            return createIndex();
        } catch (RecordIOException | IOException e) {
            throw new IndexBuildException("Failed to build index", e);
        }
    }

    private Index createIndex() throws IOException {
        Index index = new HashIndex();

        long currentOffset = 0;
        while (currentOffset < readChannel.size()) {
            ReadRequest readRequest = new ReadRequest(currentOffset);
            ReadResult readResult = recordReader.read(readRequest);

            ByteKey key = new ByteKey(readResult.recordPayload().key());
            if (readResult.recordType() == RecordType.TOMBSTONE) {
                index.markDeleted(key);
            } else {
                index.markPresent(key, currentOffset);
            }

            currentOffset = readResult.nextRecordOffset();
        }

        return index;
    }
}
