package com.ryszardzmija.storage.hash.segment.files;

import java.nio.file.Path;

class FileFormatInfo {
    private static final String WRITE_FILE_FORMAT = "segment_%019d.log";
    private static final String READ_FILE_REGEX = "segment_\\d+\\.log";
    private static final long FIRST_ID = 1;

    private static final String extractIdErrorMessage = "Failure parsing segment filename, given: %s, expected: " + READ_FILE_REGEX;

    static String getWriteFileFormat() {
        return WRITE_FILE_FORMAT;
    }

    static String getReadFileRegex() {
        return READ_FILE_REGEX;
    }

    static long getFirstId() { return FIRST_ID; }

    static long extractId(Path file) {
        String filename = file.getFileName().toString();

        int start = filename.indexOf("_");
        int end = filename.indexOf(".");

        if (start == -1 || end == -1) {
            throw new IllegalArgumentException(extractIdErrorMessage.formatted(filename));
        }

        String idStr = filename.substring(start + 1, end);

        try {
            return Long.parseLong(idStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(extractIdErrorMessage.formatted(filename), e);
        }
    }
}