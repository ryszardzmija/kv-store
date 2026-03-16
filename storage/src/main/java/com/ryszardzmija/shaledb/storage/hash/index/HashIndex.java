package com.ryszardzmija.shaledb.storage.hash.index;

import java.util.*;

public class HashIndex implements Index {
    private final Map<ByteKey, Long> offsetMap;
    private final Set<ByteKey> deletedSet;

    public HashIndex() {
        this.offsetMap = new HashMap<>();
        this.deletedSet = new HashSet<>();
    }

    @Override
    public void markPresent(ByteKey key, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Key offset cannot be negative, got: " + offset);
        }
        offsetMap.put(key, offset);
        deletedSet.remove(key);
    }

    @Override
    public void markDeleted(ByteKey key) {
        deletedSet.add(key);
        offsetMap.remove(key);
    }

    @Override
    public boolean isDeleted(ByteKey key) {
        return deletedSet.contains(key);
    }

    @Override
    public Optional<Long> getKeyOffset(ByteKey key) {
        return Optional.ofNullable(offsetMap.get(key));
    }
}
