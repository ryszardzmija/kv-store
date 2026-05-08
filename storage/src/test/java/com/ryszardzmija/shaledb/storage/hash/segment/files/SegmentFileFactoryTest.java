package com.ryszardzmija.shaledb.storage.hash.segment.files;

import com.ryszardzmija.shaledb.storage.durability.DurabilityException;
import com.ryszardzmija.shaledb.storage.durability.FileSystemSync;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SegmentFileFactoryTest {
    @TempDir
    Path tempDir;

    FileSystemSync fileSystemSync;
    SegmentFileFactory factory;

    @BeforeEach
    void setUp() {
        fileSystemSync = mock(FileSystemSync.class);
        factory = new SegmentFileFactory(tempDir, fileSystemSync);
    }

    @Test
    void createsFirstSegmentFileInEmptyDirectory() throws IOException {
        Path segmentFile = factory.createSegmentFile();

        assertThat(segmentFile.getParent()).isEqualTo(tempDir);
        assertThat(segmentFile.getFileName().toString()).isEqualTo(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId()));
        assertThat(segmentFile).exists().isRegularFile();

        try (Stream<Path> files = Files.list(tempDir)) {
            assertThat(files).containsExactlyInAnyOrder(segmentFile);
        }
    }

    @Test
    void createsSegmentFileAfterHighestExistingId() throws IOException {
        Files.createFile(tempDir.resolve(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId())));
        Files.createFile(tempDir.resolve(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId() + 1)));
        // segment directory is scanned at object creation
        factory = new SegmentFileFactory(tempDir, fileSystemSync);

        Path segmentFile = factory.createSegmentFile();

        assertThat(segmentFile.getFileName().toString()).isEqualTo(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId() + 2));
        assertThat(segmentFile).exists().isRegularFile();
    }

    @Test
    void flushesSegmentDirectoryAfterCreatingSegmentFile() {
        factory.createSegmentFile();

        verify(fileSystemSync).forceDirectory(tempDir);
    }

    @Test
    void doesNotReuseIdAfterFailedDirectoryFlush() {
        doThrow(new DurabilityException("error", new IOException("error")))
                .doNothing()
                .doNothing()
                .when(fileSystemSync)
                .forceDirectory(tempDir);
        Path segmentFile = tempDir.resolve(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId()));

        assertThatThrownBy(() -> factory.createSegmentFile())
                .isInstanceOf(SegmentFileException.class)
                .hasCauseInstanceOf(DurabilityException.class)
                .hasMessage("Failed to flush segment file creation: " + segmentFile);

        Path nextSegmentFile = factory.createSegmentFile().getFileName();

        assertThat(nextSegmentFile.toString()).isEqualTo(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId() + 1));
        verify(fileSystemSync, times(3)).forceDirectory(tempDir);
    }

    @Test
    void usesMonotonicallyIncreasingIds() {
        Path first = factory.createSegmentFile().getFileName();
        Path second = factory.createSegmentFile().getFileName();

        assertThat(first.toString()).isEqualTo(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId()));
        assertThat(second.toString()).isEqualTo(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId() + 1));
    }

    @Test
    void deletesCreatedSegmentFileAfterFailedDirectoryFlush() throws IOException {
        doThrow(new DurabilityException("error", new IOException("error")))
                .doNothing()
                .when(fileSystemSync)
                .forceDirectory(tempDir);
        Path segmentFile = tempDir.resolve(FileFormatInfo.getWriteFileFormat().formatted(FileFormatInfo.getFirstId()));

        assertThatThrownBy(() -> factory.createSegmentFile())
                .isInstanceOf(SegmentFileException.class)
                .hasCauseInstanceOf(DurabilityException.class)
                .hasMessage("Failed to flush segment file creation: " + segmentFile);

        try (Stream<Path> files = Files.list(tempDir)) {
            assertThat(files.toList()).isEmpty();
        }

        verify(fileSystemSync, times(2)).forceDirectory(tempDir);
    }

    @Test
    void rejectsPathThatIsNotDirectory() throws IOException {
        Path filePath = Files.createFile(tempDir.resolve("temp"));

        assertThatThrownBy(() -> new SegmentFileFactory(filePath, fileSystemSync))
                .isInstanceOf(SegmentFileException.class)
                .hasMessage(filePath + " is not a directory, expected a directory with segment files");
    }
}
