package com.ryszardzmija.storage.hash.index;

import java.util.Optional;

public interface Index {
    boolean containsKey(ByteKey key);
    Optional<Long> getKeyOffset(ByteKey key);
    void putKeyOffset(ByteKey key, long offset);
}
