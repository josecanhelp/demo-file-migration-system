package com.filemigration.worker;

import com.filemigration.store.ObjectStore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * In-memory stand-in for ObjectStore. Tracks how many times put was
 * called and what landed under each key, and which keys were deleted,
 * without touching any real object storage.
 */
class FakeObjectStore extends ObjectStore {

    private final Map<String, byte[]> objects = new HashMap<>();
    private final Set<String> deletedKeys = new HashSet<>();
    private int putCount = 0;
    private RuntimeException nextPutFailure;
    private RuntimeException nextDeleteFailure;

    FakeObjectStore() {
        super(null, "documents");
    }

    int putCount() {
        return putCount;
    }

    boolean wasDeleted(String key) {
        return deletedKeys.contains(key);
    }

    @Override
    public void delete(String key) {
        if (nextDeleteFailure != null) {
            RuntimeException toThrow = nextDeleteFailure;
            nextDeleteFailure = null;
            throw toThrow;
        }
        objects.remove(key);
        deletedKeys.add(key);
    }

    /**
     * The very next call to delete() throws this instead of removing
     * anything, simulating an infrastructure problem unrelated to the
     * ledger tombstone it is paired with.
     */
    void throwOnNextDelete(RuntimeException exception) {
        this.nextDeleteFailure = exception;
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
