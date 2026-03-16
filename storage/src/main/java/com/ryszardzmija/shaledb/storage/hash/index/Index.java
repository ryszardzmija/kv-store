package com.ryszardzmija.shaledb.storage.hash.index;

import java.util.Optional;

public interface Index {
    /**
     * Marks the key as present and stores its offset within the segment.
     *
     * @param key the key to be marked and stored
     * @param offset the offset of the record for the key within the segment
     * @throws IllegalArgumentException if the offset is negative
     */
    void markPresent(ByteKey key, long offset);

    /**
     * Marks the key as deleted.
     *
     * @param key the key to be marked
     */
    void markDeleted(ByteKey key);

    /**
     * Checks if a key is deleted.
     *
     * @param key the key to be checked
     * @return whether the key is marked as deleted
     */
    boolean isDeleted(ByteKey key);

    /**
     * Checks if a key is present and retrieves its offset.
     *
     * @param key the key used for checking and retrieving
     * @return the offset if present, {@link Optional#empty()} if not present or deleted
     */
    Optional<Long> getKeyOffset(ByteKey key);
}
