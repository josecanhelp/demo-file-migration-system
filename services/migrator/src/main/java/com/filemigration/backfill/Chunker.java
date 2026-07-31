package com.filemigration.backfill;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits an ordered list of ids into fixed-size chunks, preserving order
 * and dropping nothing. The final chunk holds whatever remainder is left
 * over rather than being padded or merged into its neighbor, so a range
 * whose size does not divide evenly still accounts for every id exactly
 * once.
 */
public final class Chunker {

    private Chunker() {
    }

    public static List<List<Long>> chunk(List<Long> ids, int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive, got " + chunkSize);
        }
        List<List<Long>> chunks = new ArrayList<>();
        for (int i = 0; i < ids.size(); i += chunkSize) {
            chunks.add(List.copyOf(ids.subList(i, Math.min(i + chunkSize, ids.size()))));
        }
        return chunks;
    }
}
