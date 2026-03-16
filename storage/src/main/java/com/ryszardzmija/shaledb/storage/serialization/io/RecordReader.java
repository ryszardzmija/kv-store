package com.ryszardzmija.shaledb.storage.serialization.io;

import com.ryszardzmija.shaledb.storage.config.StorageConfigHolder;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordType;
import com.ryszardzmija.shaledb.storage.serialization.spec.FormatInfo;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordPayload;
import com.ryszardzmija.shaledb.storage.serialization.spec.RecordTypeCodec;
import com.ryszardzmija.shaledb.storage.serialization.spec.TypeDecodingException;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class RecordReader {
    private final FileChannel readChannel;

    public RecordReader(FileChannel readChannel) {
        this.readChannel = Objects.requireNonNull(readChannel);
    }

    public ReadResult read(ReadRequest request) {
        long offset = request.offset();

        try {
            ByteBuffer headerBuffer = ByteBuffer.allocate(FormatInfo.getHeaderSize()).order(FormatInfo.BYTE_ORDER);
            readIntoBuffer(headerBuffer, offset);
            headerBuffer.flip();

            byte[] storedChecksum = new byte[FormatInfo.CHECKSUM_FIELD_SIZE];
            headerBuffer.get(storedChecksum);

            byte typeValue = headerBuffer.get();
            RecordType recordType = RecordTypeCodec.decode(typeValue);

            int keyDataSize = headerBuffer.getInt();
            int valueDataSize = headerBuffer.getInt();

            if (!isLengthValid((long) keyDataSize + valueDataSize)) {
                throw new DataCorruptionException("Retrieved record length is invalid", offset);
            }

            ByteBuffer dataBuffer = ByteBuffer.allocate(keyDataSize + valueDataSize).order(FormatInfo.BYTE_ORDER);
            readIntoBuffer(dataBuffer, offset + FormatInfo.getHeaderSize());

            headerBuffer.rewind();
            headerBuffer.position(FormatInfo.CHECKSUM_FIELD_SIZE);
            dataBuffer.flip();
            Checksum checksum = new CRC32C();
            checksum.update(headerBuffer);
            checksum.update(dataBuffer);
            if (!isChecksumValid(storedChecksum, checksum)) {
                throw new DataCorruptionException("Invalid checksum", offset);
            }

            byte[] keyData = new byte[keyDataSize];
            byte[] valueData = new byte[valueDataSize];
            dataBuffer.rewind();
            dataBuffer.get(keyData);
            dataBuffer.get(valueData);

            long nextRecordOffset = offset + FormatInfo.getHeaderSize() + keyDataSize + valueDataSize;

            return new ReadResult(new RecordPayload(keyData, valueData), recordType, nextRecordOffset);
        } catch (BufferUnderflowException e) {
            throw new DataCorruptionException("Unexpected end of record data", offset, e);
        } catch (TypeDecodingException e) {
            throw new DataCorruptionException("Error decoding record type", offset, e);
        } catch (IOException e) {
            throw new RecordIOException("Error reading record data at offset: " + offset, e);
        }
    }

    private void readIntoBuffer(ByteBuffer buffer, long offset) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = readChannel.read(buffer, offset + buffer.position());
            if (bytesRead == -1) {
                throw new DataCorruptionException("Unexpected end of record data", offset);
            }
        }
    }

    private boolean isLengthValid(long length) {
        return length <= StorageConfigHolder.get().maxRecordSize() - FormatInfo.getHeaderSize();
    }

    private boolean isChecksumValid(byte[] storedChecksum, Checksum computedChecksum) {
        long storedChecksumLong = Integer.toUnsignedLong(ByteBuffer.wrap(storedChecksum).order(FormatInfo.BYTE_ORDER).getInt());
        return computedChecksum.getValue() == storedChecksumLong;
    }
}
