package com.filemigration.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

/**
 * Builds the S3 client the migrator uses to talk to MinIO. Path-style
 * access is required: without it the client addresses buckets as
 * subdomains (bucket.minio:9000), which MinIO's default setup does not
 * resolve. The region is a placeholder the SDK requires even though MinIO
 * does not use it.
 */
@Configuration
public class ObjectStoreConfig {

    @Bean
    public S3Client s3Client(
            @Value("${migrator.object-store.endpoint}") String endpoint,
            @Value("${migrator.object-store.access-key}") String accessKey,
            @Value("${migrator.object-store.secret-key}") String secretKey,
            @Value("${migrator.object-store.region}") String region) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }
}
