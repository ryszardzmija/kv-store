package com.ryszardzmija.storage.hash.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HashIndex implements Index {
    private final Map<ByteKey, Long> offsetMap;

    public HashIndex() {
        this.offsetMap = new HashMap<>();
    }

    @Override
    public boolean containsKey(ByteKey key) {
        return offsetMap.containsKey(key);
    }

    @Override
    public Optional<Long> getKeyOffset(ByteKey key) {
        if (offsetMap.containsKey(key)) {
            return Optional.of(offsetMap.get(key));
        }

        return Optional.empty();
    }

    @Override
    public void putKeyOffset(ByteKey key, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Key offset cannot be negative, got: " + offset);
        }
        offsetMap.put(key, offset);
    }
}
