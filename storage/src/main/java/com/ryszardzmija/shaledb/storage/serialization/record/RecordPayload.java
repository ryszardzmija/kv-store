package com.ryszardzmija.shaledb.storage.serialization.record;

public record RecordPayload(byte[] key, byte[] value) {
    public RecordPayload {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Record key must not be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Record value must not be null");
        }
    }

    public static RecordPayload forTombstone(byte[] key) {
        return new RecordPayload(key, new byte[0]);
    }
}
