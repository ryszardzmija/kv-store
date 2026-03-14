package com.ryszardzmija.segment.model;

import com.ryszardzmija.format.RecordIOException;
import com.ryszardzmija.format.RecordReader;
import com.ryszardzmija.index.ByteKey;
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

public class ImmutableSegment implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ImmutableSegment.class);

    private final Path path;
    private final FileChannel readChannel;
    private final SegmentReader segmentReader;

    public ImmutableSegment(Path path) {
        this.path = Objects.requireNonNull(path);

        FileChannel openedReadChannel = null;
        try {
            openedReadChannel = FileChannel.open(path, StandardOpenOption.READ);

            this.readChannel = openedReadChannel;
            this.segmentReader = new SegmentReader(new RecordReader(readChannel), new IndexBuilder(readChannel).build());
        } catch (IndexBuildException | IOException e) {
            if (openedReadChannel != null) {
                try {
                    openedReadChannel.close();
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

    public long size() {
        try {
            return readChannel.size();
        } catch (IOException e) {
            throw new SegmentIOException("Failed to get size of segment " + path, e);
        }
    }

    @Override
    public void close() {
        try {
            readChannel.close();
        } catch (IOException e) {
            logger.warn("Failed to close read channel of segment {}", path, e);
        }
    }
}
