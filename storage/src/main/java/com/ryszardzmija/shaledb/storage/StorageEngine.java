package com.ryszardzmija.shaledb.storage;

import java.util.Optional;

/**
 * A storage engine for key-value pairs.
 *
 * <p>All mutating operations are idempotent when replayed with identical arguments.
 * Repeating the same operation produces the same observable state as executing it once.
 */
public interface StorageEngine {
    /**
     * Stores the key-value pair. If the key already exists, the value is overwritten.
     *
     * <p>Idempotent for identical key-value pairs. Note that calling put(key, v1) followed
     * by put(key, v2) is not a replay. These are two distinct operations resulting in the value of v2.
     *
     * @param key a non-empty array of bytes representing the key
     * @param value a non-null array of bytes representing the value; may be empty
     * @throws IllegalArgumentException if the key or value is null; if the key is empty
     * @throws StorageEngineException if the key-value pair fails to be written
     */
    void put(byte[] key, byte[] value);

    /**
     * Deletes the key-value pair associated with the key.
     *
     * <p>Idempotent since deleting a key that does not exist or was already deleted
     * leaves the store in the same state.
     *
     * @param key a non-empty array of bytes representing the key
     * @throws IllegalArgumentException if the key is null or empty
     * @throws StorageEngineException if the key-value pair associated with the key fails to be deleted
     */
    void delete(byte[] key);

    /**
     * Retrieves the value associated with the key.
     *
     * @param key a non-empty array of bytes representing the key
     * @return the value associated with the given key, or {@link Optional#empty()} if the key does not exist
     * @throws IllegalArgumentException if the key is null or empty
     * @throws StorageEngineException if the value associated with the key fails to be retrieved
     */
    Optional<byte[]> get(byte[] key);

    /**
     * Releases all resources held by the storage engine.
     *
     * <p>After calling this method, later calls to {@link #put(byte[], byte[])} or
     * {@link #get(byte[])} will result in undefined behavior. Implementations should
     * ensure that all buffered data is flushed to persistent storage before returning.
     */
    void close();
}
