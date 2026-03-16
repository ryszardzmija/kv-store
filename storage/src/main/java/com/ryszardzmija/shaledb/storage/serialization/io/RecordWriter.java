package com.ryszardzmija.shaledb.storage.serialization.io;

import com.ryszardzmija.shaledb.storage.serialization.spec.FormatInfo;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordPayload;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordType;
import com.ryszardzmija.shaledb.storage.serialization.spec.RecordTypeCodec;

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

    public WriteResult write(WriteRequest request) {
        try {
            RecordPayload recordPayload = request.payload();
            RecordType recordType = request.type();

            int keyLength = recordPayload.key().length;
            int valueLength = recordPayload.value().length;
            byte encodedType = RecordTypeCodec.encode(recordType);

            ByteBuffer buffer = ByteBuffer.allocate(FormatInfo.getHeaderSize() + keyLength + valueLength).order(FormatInfo.BYTE_ORDER);

            ByteBuffer checksumHeaderFields = ByteBuffer.allocate(FormatInfo.getChecksumHeaderFieldsSize()).order(FormatInfo.BYTE_ORDER);
            checksumHeaderFields.put(encodedType);
            checksumHeaderFields.putInt(keyLength);
            checksumHeaderFields.putInt(valueLength);
            checksumHeaderFields.flip();
            Checksum checksum = new CRC32C();
            checksum.update(checksumHeaderFields);
            checksum.update(recordPayload.key());
            checksum.update(recordPayload.value());
            long checksumValue = checksum.getValue();

            buffer.putInt((int) checksumValue);
            buffer.put(encodedType);
            buffer.putInt(recordPayload.key().length);
            buffer.putInt(recordPayload.value().length);
            buffer.put(recordPayload.key());
            buffer.put(recordPayload.value());
            buffer.flip();

            WriteResult result = new WriteResult(writeChannel.position());
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
