package com.ryszardzmija.storage.hash.index;

import com.ryszardzmija.storage.format.RecordIOException;
import com.ryszardzmija.storage.format.RecordReader;

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
            var readRecord = recordReader.read(currentOffset);
            ByteKey key = new ByteKey(readRecord.record().key());
            index.putKeyOffset(key, currentOffset);
            currentOffset = readRecord.nextRecordOffset();
        }

        return index;
    }
}
