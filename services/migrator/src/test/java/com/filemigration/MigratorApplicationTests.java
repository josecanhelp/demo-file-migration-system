package com.filemigration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Loads the full Spring context under the worker profile, proving that the
 * two datasources, the object store, and the store layer beans all wire
 * together without a running database or MinIO, since no bean here opens a
 * connection eagerly. The Kafka bootstrap address is overridden to a
 * loopback address that resolves with nothing listening on it, rather
 * than the default broker hostname that only exists inside the compose
 * network: the backfill consumer's listener container needs an address
 * it can resolve to start up, but actually reaching a broker only happens
 * later, on its background poll thread, so no running Kafka is needed
 * here either. The admin operation timeout is shortened for this test
 * only, so the topic-check call that has nothing to talk to gives up in
 * a couple of seconds instead of the full production timeout.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:19092",
        "spring.kafka.admin.operation-timeout=2s"
})
@ActiveProfiles("worker")
class MigratorApplicationTests {

    @Test
    void contextLoads() {
    }
}
