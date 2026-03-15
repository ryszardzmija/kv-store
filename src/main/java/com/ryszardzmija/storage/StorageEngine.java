package com.ryszardzmija.storage;

import java.util.Optional;

public interface StorageEngine {
    /**
     * Stores the key-value pair.
     *
     * @param key a non-empty array of bytes representing the key
     * @param value a non-null array of bytes representing the value; may be empty
     * @throws IllegalArgumentException if the key or value is null; if the key is empty
     * @throws StorageEngineException if the key-value pair fails to be written
     */
    void put(byte[] key, byte[] value);

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
     *
     * <p>This method is idempotent and safe to call multiple times.
     */
    void close();
}
