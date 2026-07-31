package com.filemigration.store;

import com.filemigration.model.Stage;
import com.filemigration.model.Status;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Proves the store layer types compile and wire together, and that the
 * Status and Stage values, along with the object key scheme, match exactly
 * what every later task builds on.
 */
class StoreLayerWiringTest {

    @Test
    void statusValuesMatchExactly() {
        Status[] values = Status.values();
        assertEquals(6, values.length);
        assertEquals(Status.PENDING, values[0]);
        assertEquals(Status.IN_FLIGHT, values[1]);
        assertEquals(Status.OCR_DONE, values[2]);
        assertEquals(Status.DONE, values[3]);
        assertEquals(Status.FAILED_RETRYABLE, values[4]);
        assertEquals(Status.FAILED_PERMANENT, values[5]);
    }

    @Test
    void stageValuesMatchExactly() {
        Stage[] values = Stage.values();
        assertEquals(9, values.length);
        assertEquals(Stage.CDC_CAPTURED, values[0]);
        assertEquals(Stage.QUEUED, values[1]);
        assertEquals(Stage.CLAIMED, values[2]);
        assertEquals(Stage.OCR_DONE, values[3]);
        assertEquals(Stage.STORED, values[4]);
        assertEquals(Stage.RETRY, values[5]);
        assertEquals(Stage.DLQ, values[6]);
        assertEquals(Stage.BREAKER_OPEN, values[7]);
        assertEquals(Stage.BREAKER_CLOSED, values[8]);
    }

    @Test
    void objectKeySchemeIsDeterministic() {
        ObjectStore objectStore = new ObjectStore(mock(S3Client.class), "documents");
        assertEquals("files/42", objectStore.keyFor(42L));
        assertEquals(objectStore.keyFor(42L), objectStore.keyFor(42L));
    }
}
