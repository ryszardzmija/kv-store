package com.ryszardzmija.storage.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class RecordWriter {
    private final FileChannel writeChannel;

    public RecordWriter(FileChannel writeChannel) {
        this.writeChannel = Objects.requireNonNull(writeChannel);
    }

    public RecordWriteResult write(Record record) {
        try {
            int keyLength = record.key().length;
            int valueLength = record.value().length;

            ByteBuffer buffer = ByteBuffer.allocate(FormatInfo.getHeaderSize() + keyLength + valueLength).order(FormatInfo.BYTE_ORDER);

            ByteBuffer checksumHeaderFields = ByteBuffer.allocate(Integer.BYTES * 2).order(FormatInfo.BYTE_ORDER);
            checksumHeaderFields.putInt(keyLength);
            checksumHeaderFields.putInt(valueLength);
            checksumHeaderFields.flip();
            Checksum checksum = new CRC32C();
            checksum.update(checksumHeaderFields);
            checksum.update(record.key());
            checksum.update(record.value());
            long checksumValue = checksum.getValue();

            buffer.putInt((int) checksumValue);
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
            throw new RecordIOException("Error writing record data", e);
        }
    }

    private void writeFromBuffer(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int _ = writeChannel.write(buffer);
        }
    }
}
