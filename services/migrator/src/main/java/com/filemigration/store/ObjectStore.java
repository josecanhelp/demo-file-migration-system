package com.filemigration.store;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Reads and writes file blobs in the MinIO bucket. Object keys follow a
 * single deterministic scheme so a repeat put for the same source file
 * overwrites the same object rather than creating a duplicate.
 */
@Repository
public class ObjectStore {

    private static final String KEY_PREFIX = "files/";

    private final S3Client s3Client;
    private final String bucket;

    public ObjectStore(S3Client s3Client, @Value("${migrator.object-store.bucket}") String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    /**
     * The single place that builds an object key from a source id, so no
     * other code hand-builds the key string.
     */
    public String keyFor(long sourceId) {
        return KEY_PREFIX + sourceId;
    }

    public String put(String key, byte[] bytes, String contentType) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(contentType)
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(bytes));
        return key;
    }

    public void delete(String key) {
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
    }

    public byte[] get(String key) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request)) {
            return response.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read object " + key + " from bucket " + bucket, e);
        }
    }
}
