package com.ryszardzmija.shaledb.storage.hash.segment.model;

import com.ryszardzmija.shaledb.storage.durability.DurabilityMode;
import com.ryszardzmija.shaledb.storage.hash.index.ByteKey;
import com.ryszardzmija.shaledb.storage.hash.index.Index;
import com.ryszardzmija.shaledb.storage.serialization.io.RecordIOException;
import com.ryszardzmija.shaledb.storage.serialization.io.RecordWriter;
import com.ryszardzmija.shaledb.storage.serialization.io.WriteRequest;
import com.ryszardzmija.shaledb.storage.serialization.io.WriteResult;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordPayload;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SegmentWriterTest {
    RecordWriter recordWriter;
    Index index;
    SegmentWriter segmentWriter;

    @BeforeEach
    void setUp() {
        recordWriter = mock(RecordWriter.class);
        index = mock(Index.class);
        segmentWriter = new SegmentWriter(recordWriter, index, DurabilityMode.SYNC_EACH_WRITE);
    }

    @Test
    void successfulPutExecutesCompleteFlow() {
        ByteKey key = new ByteKey(new byte[] {1});
        byte[] value = new byte[] {2};
        long offset = 0L;

        when(recordWriter.write(any(WriteRequest.class)))
                .thenReturn(new WriteResult(offset));

        segmentWriter.put(key, value);

        InOrder inOrder = inOrder(recordWriter, index);
        inOrder.verify(recordWriter).write(any(WriteRequest.class));
        inOrder.verify(recordWriter).flushToStorage();
        inOrder.verify(index).markPresent(key, offset);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void putWritesNormalRecord() {
        ByteKey key = new ByteKey(new byte[] {1});
        byte[] value = new byte[] {2};
        long offset = 0L;

        ArgumentCaptor<WriteRequest> requestCaptor = ArgumentCaptor.forClass(WriteRequest.class);
        when(recordWriter.write(requestCaptor.capture()))
                .thenReturn(new WriteResult(offset));

        segmentWriter.put(key, value);

        WriteRequest request = requestCaptor.getValue();
        assertThat(request.type()).isEqualTo(RecordType.NORMAL);
        assertThat(request.payload().key()).isEqualTo(key.getData());
        assertThat(request.payload().value()).isEqualTo(value);
    }

    @Test
    void putWriteFailureSkipsFlushingAndIndexUpdate() {
        ByteKey key = new ByteKey(new byte[] {1});
        byte[] value = new byte[] {2};
        RecordIOException exception = new RecordIOException("write failed", new IOException("error"));

        when(recordWriter.write(any(WriteRequest.class)))
                .thenThrow(exception);

        assertThatThrownBy(() -> segmentWriter.put(key, value))
                .isSameAs(exception);

        verify(recordWriter, never()).flushToStorage();
        verify(index, never()).markPresent(any(), anyLong());
        verify(index, never()).markDeleted(any());
    }

    @Test
    void putFlushFailureSkipsIndexUpdate() {
        ByteKey key = new ByteKey(new byte[] {1});
        byte[] value = new byte[] {2};
        long offset = 0L;
        RecordIOException exception = new RecordIOException("flush failed", new IOException("error"));

        when(recordWriter.write(any(WriteRequest.class)))
                .thenReturn(new WriteResult(offset));
        doThrow(exception).when(recordWriter).flushToStorage();

        assertThatThrownBy(() -> segmentWriter.put(key, value))
                .isSameAs(exception);

        verify(recordWriter).write(any(WriteRequest.class));
        verify(index, never()).markPresent(any(), anyLong());
        verify(index, never()).markDeleted(any());
    }

    @Test
    void successfulDeleteExecutesCompleteFlow() {
        ByteKey key = new ByteKey(new byte[] {1});

        segmentWriter.delete(key);

        InOrder inOrder = inOrder(recordWriter, index);
        inOrder.verify(recordWriter).write(any(WriteRequest.class));
        inOrder.verify(recordWriter).flushToStorage();
        inOrder.verify(index).markDeleted(key);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void deleteWritesTombstoneRecord() {
        ByteKey key = new ByteKey(new byte[] {1});
        long offset = 0L;

        ArgumentCaptor<WriteRequest> requestCaptor = ArgumentCaptor.forClass(WriteRequest.class);
        when(recordWriter.write(requestCaptor.capture()))
                .thenReturn(new WriteResult(offset));

        segmentWriter.delete(key);

        WriteRequest request = requestCaptor.getValue();
        assertThat(request.type()).isEqualTo(RecordType.TOMBSTONE);
        assertThat(request.payload().key()).isEqualTo(key.getData());
        assertThat(request.payload().value()).isEqualTo(RecordPayload.forTombstone(key.getData()).value());
    }

    @Test
    void deleteWriteFailureSkipsFlushingAndIndexUpdate() {
        ByteKey key = new ByteKey(new byte[] {1});
        RecordIOException exception = new RecordIOException("write failed", new IOException("error"));

        when(recordWriter.write(any(WriteRequest.class)))
                .thenThrow(exception);

        assertThatThrownBy(() -> segmentWriter.delete(key))
                .isSameAs(exception);

        verify(recordWriter, never()).flushToStorage();
        verify(index, never()).markPresent(any(), anyLong());
        verify(index, never()).markDeleted(any());
    }

    @Test
    void deleteFlushFailureSkipsIndexUpdate() {
        ByteKey key = new ByteKey(new byte[] {1});
        long offset = 0L;
        RecordIOException exception = new RecordIOException("flush failed", new IOException("error"));

        when(recordWriter.write(any(WriteRequest.class)))
                .thenReturn(new WriteResult(offset));
        doThrow(exception).when(recordWriter).flushToStorage();

        assertThatThrownBy(() -> segmentWriter.delete(key))
                .isSameAs(exception);

        verify(recordWriter).write(any(WriteRequest.class));
        verify(index, never()).markPresent(any(), anyLong());
        verify(index, never()).markDeleted(any());
    }
}
