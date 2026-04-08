package com.sync.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sync.dto.SyncResponse;
import com.sync.dto.SyncStatusResponse;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SyncApiClient {

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private boolean isOnline = true;

    public SyncApiClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public boolean isOnline() {
        return isOnline;
    }

    public SyncResponse syncTable(String tableName, List<Map<String, Object>> records) throws Exception {
        String url = baseUrl + "/api/sync/" + tableName;
        String json = objectMapper.writeValueAsString(records);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .timeout(Duration.ofMinutes(1))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            isOnline = true; // Mark as online on successful response

            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), SyncResponse.class);
            } else if (response.statusCode() == 400) {
                SyncResponse syncResponse = objectMapper.readValue(response.body(), SyncResponse.class);
                throw new ValidationException(syncResponse.getErrors());
            } else {
                throw new IOException("Sync failed with status code: " + response.statusCode());
            }
        } catch (java.net.ConnectException | java.net.http.HttpConnectTimeoutException | java.net.http.HttpTimeoutException e) {
            isOnline = false; // Trigger offline mode
            throw e;
        }
    }

    public SyncStatusResponse getSyncStatus(String tableName) throws Exception {
        String url = baseUrl + "/api/sync/status/" + tableName;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            return objectMapper.readValue(response.body(), SyncStatusResponse.class);
        } else if (response.statusCode() == 404) {
            return null;
        } else {
            throw new RuntimeException("Status fetch failed with status code: " + response.statusCode());
        }
    }

    private <T> T executeWithRetry(CallableWithException<T> task, int maxRetries, long delayMs) throws Exception {
        int attempt = 0;
        while (true) {
            try {
                return task.call();
            } catch (ValidationException e) {
                // Don't retry on validation errors
                throw e;
            } catch (Exception e) {
                attempt++;
                if (attempt >= maxRetries) {
                    throw e;
                }
                log.warn("Attempt {} failed, retrying in {}ms: {}", attempt, delayMs, e.getMessage());
                TimeUnit.MILLISECONDS.sleep(delayMs * attempt);
            }
        }
    }

    @FunctionalInterface
    private interface CallableWithException<T> {
        T call() throws Exception;
    }

    public static class ValidationException extends RuntimeException {
        private final List<String> errors;

        public ValidationException(List<String> errors) {
            super("Validation failed: " + String.join(", ", errors));
            this.errors = errors;
        }

        public List<String> getErrors() {
            return errors;
        }
    }
}
