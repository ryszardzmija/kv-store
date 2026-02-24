package com.ryszardzmija;

import java.io.IOException;
import java.nio.file.Path;

public class Main {
    static void main(String[] args) {
        try (KeyValueStore keyValueStore = new KeyValueStore(Path.of("logfile"))) {
            keyValueStore.put("hello", "world");
            System.out.println(keyValueStore.get("hello"));
            keyValueStore.put("answer", "42");
            System.out.println(keyValueStore.get("answer"));
            keyValueStore.put("hello", "universe");
            System.out.println(keyValueStore.get("hello"));
        } catch (IOException e) {
            System.err.println("Fatal error: " + e);
            System.exit(1);
        }
    }
}
