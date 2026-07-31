package com.filemigration.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Builds the RestClient used to call the vendor OCR service. The connect
 * timeout is short since the vendor is either reachable or it isn't. The
 * read timeout is long enough to tolerate the vendor's slow-mode latency
 * but short enough that a hung connection gets abandoned instead of
 * blocking a worker thread indefinitely.
 */
@Configuration
public class VendorConfig {

    @Bean
    public RestClient vendorRestClient(
            @Value("${migrator.vendor.base-url}") String baseUrl,
            @Value("${migrator.vendor.connect-timeout-ms}") int connectTimeoutMs,
            @Value("${migrator.vendor.read-timeout-ms}") int readTimeoutMs) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(connectTimeoutMs);
        requestFactory.setReadTimeout(readTimeoutMs);
        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }
}
