package com.ryszardzmija.shaledb.storage.durability;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileSystemSync {
    public void forceFile(FileChannel channel) {
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new DurabilityException("Failed to force file contents to durable storage", e);
        }
    }

    public void forceDirectory(Path directory) {
        try (FileChannel dirChannel = FileChannel.open(directory, StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (IOException e) {
            throw new DurabilityException("Failed to force directory to durable storage: " + directory, e);
        }
    }
}
