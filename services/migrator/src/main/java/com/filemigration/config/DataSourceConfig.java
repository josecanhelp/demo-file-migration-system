package com.filemigration.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Wires the two datasources the migrator depends on: the MySQL source
 * database holding the original file blobs, and the Postgres target
 * database holding the migration ledger and the migrated document
 * metadata. The target datasource is primary since most of the migrator's
 * own bookkeeping lives there.
 *
 * Both pools share one size, read from configuration and falling back to
 * worker concurrency plus a small margin so every worker thread can hold a
 * connection with a few left over for housekeeping queries.
 */
@Configuration
public class DataSourceConfig {

    private static final int POOL_HEADROOM = 4;

    @Value("${migrator.worker-concurrency}")
    private int workerConcurrency;

    @Value("${migrator.db-pool-size}")
    private int configuredPoolSize;

    private int resolvePoolSize() {
        return configuredPoolSize > 0 ? configuredPoolSize : workerConcurrency + POOL_HEADROOM;
    }

    @Bean(name = "sourceDataSource")
    public DataSource sourceDataSource(
            @Value("${migrator.mysql.host}") String host,
            @Value("${migrator.mysql.port}") int port,
            @Value("${migrator.mysql.database}") String database,
            @Value("${migrator.mysql.username}") String username,
            @Value("${migrator.mysql.password}") String password) {
        HikariDataSource dataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .driverClassName("com.mysql.cj.jdbc.Driver")
                .url("jdbc:mysql://" + host + ":" + port + "/" + database
                        + "?useSSL=false&allowPublicKeyRetrieval=true&connectionCollation=utf8mb4_general_ci")
                .username(username)
                .password(password)
                .build();
        dataSource.setPoolName("source-pool");
        dataSource.setMaximumPoolSize(resolvePoolSize());
        return dataSource;
    }

    @Bean
    @Qualifier("sourceJdbc")
    public JdbcTemplate sourceJdbc(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        return new JdbcTemplate(sourceDataSource);
    }

    @Bean(name = "targetDataSource")
    public DataSource targetDataSource(
            @Value("${migrator.postgres.host}") String host,
            @Value("${migrator.postgres.port}") int port,
            @Value("${migrator.postgres.database}") String database,
            @Value("${migrator.postgres.username}") String username,
            @Value("${migrator.postgres.password}") String password) {
        HikariDataSource dataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .driverClassName("org.postgresql.Driver")
                .url("jdbc:postgresql://" + host + ":" + port + "/" + database)
                .username(username)
                .password(password)
                .build();
        dataSource.setPoolName("target-pool");
        dataSource.setMaximumPoolSize(resolvePoolSize());
        return dataSource;
    }

    @Bean
    @Primary
    @Qualifier("targetJdbc")
    public JdbcTemplate targetJdbc(@Qualifier("targetDataSource") DataSource targetDataSource) {
        return new JdbcTemplate(targetDataSource);
    }

    // Bound explicitly to the target datasource rather than left to be
    // inferred, since two DataSource beans exist and neither is marked
    // primary at the DataSource level (only the JdbcTemplate above is).
    // Anything in the store layer that needs more than one statement to
    // commit together, such as removing a row from more than one target
    // table, depends on this bean to make that atomic.
    @Bean
    public PlatformTransactionManager targetTransactionManager(
            @Qualifier("targetDataSource") DataSource targetDataSource) {
        return new DataSourceTransactionManager(targetDataSource);
    }
}
