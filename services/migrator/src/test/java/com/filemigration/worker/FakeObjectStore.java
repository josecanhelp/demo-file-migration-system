package com.filemigration.worker;

import com.filemigration.store.ObjectStore;

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory stand-in for ObjectStore. Tracks how many times put was
 * called and what landed under each key, without touching any real
 * object storage.
 */
class FakeObjectStore extends ObjectStore {

    private final Map<String, byte[]> objects = new HashMap<>();
    private int putCount = 0;
    private RuntimeException nextPutFailure;

    FakeObjectStore() {
        super(null, "documents");
    }

    int putCount() {
        return putCount;
    }

    /**
     * The very next call to put() throws this instead of storing
     * anything, simulating an infrastructure problem (a Postgres or
     * MinIO blip) unrelated to the vendor call.
     */
    void throwOnNextPut(RuntimeException exception) {
        this.nextPutFailure = exception;
    }

    @Override
    public byte[] get(String key) {
        return objects.get(key);
    }

    @Override
    public String put(String key, byte[] bytes, String contentType) {
        if (nextPutFailure != null) {
            RuntimeException toThrow = nextPutFailure;
            nextPutFailure = null;
            throw toThrow;
        }
        objects.put(key, bytes);
        putCount++;
        return key;
    }
}
