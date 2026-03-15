package com.ryszardzmija.storage.config;

public class StorageConfigHolder {
    private static volatile StorageConfig instance;

    /**
     * Initialize {@link StorageConfigHolder} with {@link StorageConfig}.
     *
     * <p>Not thread-safe. Must be called once from the main thread at the application startup.
     */
    public static void initialize(StorageConfig storageConfig) {
        if (instance != null) {
            throw new IllegalStateException("StorageConfig already initialized");
        }
        instance = storageConfig;
    }

    public static StorageConfig get() {
        if (instance == null) {
            throw new IllegalStateException("StorageConfig not initialized");
        }
        return instance;
    }
}
