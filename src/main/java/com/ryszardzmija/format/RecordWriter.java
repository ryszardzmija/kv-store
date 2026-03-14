package com.ryszardzmija.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class RecordWriter {
    private final FileChannel writeChannel;

    public RecordWriter(FileChannel writeChannel) {
        this.writeChannel = Objects.requireNonNull(writeChannel);
    }

    public RecordWriteResult write(Record record) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(FormatInfo.HEADER_SIZE + record.key().length + record.value().length);
            buffer.putInt(record.key().length);
            buffer.putInt(record.value().length);
            buffer.put(record.key());
            buffer.put(record.value());
            buffer.flip();

            RecordWriteResult result = new RecordWriteResult(writeChannel.position());
            // Note: writes are not fsynced per-record for performance reasons.
            // Data in the OS page cache may be lost on a hard crash.
            // This needs to be handled using a separate mechanism.
            writeFromBuffer(buffer);

            return result;
        } catch (IOException e) {
            throw new RecordIOException("Record writing record data", e);
        }
    }

    private void writeFromBuffer(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int _ = writeChannel.write(buffer);
        }
    }
}
