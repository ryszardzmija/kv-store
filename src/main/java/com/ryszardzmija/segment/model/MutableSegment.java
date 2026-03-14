package com.ryszardzmija.segment.model;

import com.ryszardzmija.format.RecordIOException;
import com.ryszardzmija.format.RecordReader;
import com.ryszardzmija.format.RecordWriter;
import com.ryszardzmija.index.ByteKey;
import com.ryszardzmija.index.Index;
import com.ryszardzmija.index.IndexBuildException;
import com.ryszardzmija.index.IndexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;

public class MutableSegment implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MutableSegment.class);

    private final Path path;
    private final FileChannel readChannel;
    private final FileChannel writeChannel;
    private final SegmentReader segmentReader;
    private final SegmentWriter segmentWriter;

    public MutableSegment(Path path) {
        this.path = Objects.requireNonNull(path);

        FileChannel openedReadChannel = null;
        FileChannel openedWriteChannel = null;
        try {
            openedReadChannel = FileChannel.open(path, StandardOpenOption.READ);
            openedWriteChannel = FileChannel.open(path, StandardOpenOption.APPEND);
            Index index = new IndexBuilder(openedReadChannel).build();

            this.readChannel = openedReadChannel;
            this.writeChannel = openedWriteChannel;
            this.segmentReader = new SegmentReader(new RecordReader(readChannel), index);
            this.segmentWriter = new SegmentWriter(new RecordWriter(writeChannel), index);
        } catch (IndexBuildException | IOException e) {
            if (openedReadChannel != null) {
                try {
                    openedReadChannel.close() ;
                } catch (IOException ignored) {}
            }
            if (openedWriteChannel != null) {
                try {
                    openedWriteChannel.close();
                } catch (IOException ignored) {}
            }

            if (e instanceof IndexBuildException ie) {
                throw new SegmentIOException("Failed to create index for segment file " + path, ie);
            }

            throw new SegmentIOException("Failed to open the segment file " + path, e);
        }
    }

    public Optional<byte[]> get(ByteKey key) {
        try {
            return segmentReader.get(key);
        } catch (RecordIOException e) {
            throw new SegmentIOException("Failed to read from segment " + path, e);
        }
    }

    public void put(ByteKey key, byte[] value) {
        try {
            segmentWriter.put(key, value);
        } catch (RecordIOException e) {
            throw new SegmentIOException("Failed to write to segment " + path, e);
        }
    }

    public long size() {
        try {
            return readChannel.size();
        } catch (IOException e) {
            throw new SegmentIOException("Failed to get size of segment " + path, e);
        }
    }

    public Path path() {
        return path;
    }

    @Override
    public void close() {
        try {
            readChannel.close();
        } catch (IOException e) {
            logger.warn("Failed to close read channel of segment {}", path, e);
        }

        try {
            writeChannel.close();
        } catch (IOException e) {
            logger.warn("Failed to close write channel of segment {}", path, e);
        }
    }
}
