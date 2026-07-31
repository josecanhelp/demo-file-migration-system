package com.filemigration.model;

import java.time.Instant;

/**
 * A single row read from the source database's files table: the blob itself
 * plus the metadata needed to write it into the target store.
 */
public record FileRecord(
        long id,
        String filename,
        String contentType,
        byte[] content,
        int byteSize,
        Instant createdAt
) {
}
