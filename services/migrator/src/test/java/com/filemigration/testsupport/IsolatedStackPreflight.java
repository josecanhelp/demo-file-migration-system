package com.filemigration.testsupport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Refuses to let an integration test class start while a containerized
 * migrator-worker or migrator-coordinator is running. Both write real rows
 * into MySQL sourcedb.files, which the real Debezium connector captures
 * into the real cdc.sourcedb.files topic regardless of which suite's id
 * range they land in. A live worker or coordinator container consumes
 * that topic, and the backfill topic, on its own schedule and its own
 * retry cap, entirely outside any test's own lifecycle, and can claim and
 * condemn a test's source id before the test's own consumer ever sees it.
 * Isolating topics and consumer groups per test class does not prevent
 * this, since the contamination enters through MySQL, not through Kafka.
 *
 * Each integration test class that writes directly to the source database
 * calls {@link #verify()} once, as early as its own lifecycle allows, so
 * this fails loudly before any fixture is seeded rather than surfacing
 * later as an unexplained retry-cap failure. verify() only checks once per
 * JVM; every class after the first pays for a single `docker ps` call.
 */
public final class IsolatedStackPreflight {

    private static volatile boolean verified = false;

    private IsolatedStackPreflight() {
    }

    public static synchronized void verify() {
        if (verified) {
            return;
        }
        List<String> offenders = new ArrayList<>();
        for (String name : runningContainerNames()) {
            if (name.contains("migrator-worker") || name.contains("migrator-coordinator")) {
                offenders.add(name);
            }
        }
        if (!offenders.isEmpty()) {
            throw new IllegalStateException("Refusing to run the integration suite: found a running "
                    + "containerized migrator worker or coordinator (" + offenders + "). Either one consumes "
                    + "the real backfill and cdc topics independently of this test JVM and will condemn this "
                    + "suite's own source ids before it ever claims them. Stop them first: "
                    + "docker compose stop migrator-worker migrator-coordinator");
        }
        verified = true;
    }

    private static List<String> runningContainerNames() {
        List<String> names = new ArrayList<>();
        Process process;
        try {
            process = new ProcessBuilder("docker", "ps", "--format", "{{.Names}}")
                    .redirectErrorStream(true)
                    .start();
        } catch (IOException e) {
            throw new IllegalStateException("Could not run `docker ps` to check for a running migrator worker "
                    + "or coordinator; is Docker running and on PATH?", e);
        }
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    names.add(trimmed);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Could not read `docker ps` output", e);
        }
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for `docker ps`", e);
        }
        if (exitCode != 0) {
            throw new IllegalStateException("`docker ps` exited " + exitCode
                    + "; is Docker running and on PATH?");
        }
        return names;
    }
}
