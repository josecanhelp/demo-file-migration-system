package com.filemigration.backfill;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure logic test for the chunking a range of source ids goes through
 * before each piece is published to Kafka. No infrastructure involved:
 * this only proves the arithmetic and the accounting of which ids land in
 * which chunk.
 */
class ChunkerTest {

    @Test
    void chunksIntoTheExactCeilingCount() {
        List<Long> ids = idsFrom(1, 101);

        List<List<Long>> chunks = Chunker.chunk(ids, 25);

        assertEquals(5, chunks.size(), "101 ids at 25 per chunk must produce ceil(101/25) = 5 chunks");
    }

    @Test
    void finalChunkHoldsTheRemainder() {
        List<Long> ids = idsFrom(1, 101);

        List<List<Long>> chunks = Chunker.chunk(ids, 25);

        assertEquals(25, chunks.get(0).size());
        assertEquals(25, chunks.get(3).size());
        assertEquals(1, chunks.get(4).size(), "the last chunk must hold only the remainder, not be padded");
    }

    @Test
    void everyIdAppearsExactlyOnceAcrossAllChunks() {
        List<Long> ids = idsFrom(1, 101);

        List<List<Long>> chunks = Chunker.chunk(ids, 25);

        List<Long> reassembled = new ArrayList<>();
        chunks.forEach(reassembled::addAll);
        assertEquals(ids, reassembled, "concatenating the chunks in order must reproduce the original id list");
    }

    @Test
    void exactMultipleProducesNoEmptyFinalChunk() {
        List<Long> ids = idsFrom(1, 100);

        List<List<Long>> chunks = Chunker.chunk(ids, 25);

        assertEquals(4, chunks.size());
        for (List<Long> chunk : chunks) {
            assertEquals(25, chunk.size());
        }
    }

    @Test
    void emptyInputProducesNoChunks() {
        assertTrue(Chunker.chunk(List.of(), 25).isEmpty());
    }

    @Test
    void chunkSmallerThanBatchSizeProducesOneChunk() {
        List<Long> ids = idsFrom(1, 3);

        List<List<Long>> chunks = Chunker.chunk(ids, 25);

        assertEquals(1, chunks.size());
        assertEquals(ids, chunks.get(0));
    }

    @Test
    void nonPositiveChunkSizeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> Chunker.chunk(idsFrom(1, 5), 0));
    }

    private static List<Long> idsFrom(long startInclusive, long count) {
        return LongStream.range(startInclusive, startInclusive + count).boxed().toList();
    }
}
