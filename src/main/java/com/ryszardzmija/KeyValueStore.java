package com.ryszardzmija;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class KeyValueStore implements AutoCloseable {
    private final FileChannel writeChannel;
    private final Path logPath;

    public KeyValueStore(Path logPath) throws IOException {
        this.logPath = logPath;
        this.writeChannel = FileChannel.open(logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    public void put(String key, String value) throws IOException {
        String line = key + ": " + value + "\n";
        ByteBuffer byteBuffer = ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8));
        writeChannel.write(byteBuffer);
        writeChannel.force(true);
    }

    public String get(String key) throws IOException {
        try (BufferedReader bufferedReader = Files.newBufferedReader(logPath)) {
            String line;
            String result = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split(": ", 2);
                if (parts[0].equals(key)) {
                    result = parts[1];
                }
            }

            return result;
        }
    }

    @Override
    public void close() throws IOException {
        writeChannel.close();
    }
}
