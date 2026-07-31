package com.filemigration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Entry point for the migrator. The same jar runs as either the worker or
 * the coordinator, chosen at startup by which Spring profile is active.
 */
@SpringBootApplication
public class MigratorApplication {

    private static final Logger log = LoggerFactory.getLogger(MigratorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(MigratorApplication.class, args);
    }

    @Bean
    public ApplicationListener<ApplicationReadyEvent> logActiveProfile(Environment environment) {
        return event -> log.info("Migrator started, active profiles: {}",
                String.join(",", environment.getActiveProfiles()));
    }
}
