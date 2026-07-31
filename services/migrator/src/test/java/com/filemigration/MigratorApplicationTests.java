package com.filemigration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Loads the full Spring context under the worker profile, proving that the
 * two datasources, the object store, and the store layer beans all wire
 * together without a running database or MinIO, since no bean here opens a
 * connection eagerly.
 */
@SpringBootTest
@ActiveProfiles("worker")
class MigratorApplicationTests {

    @Test
    void contextLoads() {
    }
}
