package com.sync.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class OfflineQueueManager {

    private final String rootDir;
    private final Path pendingDir;
    private final Path failedDir;
    private final Path successDir;
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    public OfflineQueueManager(String rootDir) {
        this.rootDir = rootDir;
        this.pendingDir = Paths.get(rootDir, "pending");
        this.failedDir = Paths.get(rootDir, "failed");
        this.successDir = Paths.get(rootDir, "success");
        init();
    }

    private void init() {
        try {
            Files.createDirectories(pendingDir);
            Files.createDirectories(failedDir);
            Files.createDirectories(successDir);
        } catch (IOException e) {
            log.error("Failed to create queue directories", e);
        }
    }

    public void saveQueue(String tableName, List<Map<String, Object>> data) {
        try {
            String fileName = String.format("%d_%s.json", System.currentTimeMillis(), tableName);
            File file = pendingDir.resolve(fileName).toFile();

            QueueItem item = new QueueItem();
            item.setTable(tableName);
            item.setData(data);
            item.setRetryCount(0);
            item.setLastAttemptTime(null);

            mapper.writeValue(file, item);
            log.info("Saved batch to pending queue: {}", file.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to save offline queue for table: " + tableName, e);
        }
    }

    public List<File> getPendingFiles() {
        List<File> files = new ArrayList<>();
        try (Stream<Path> paths = Files.list(pendingDir)) {
            paths.filter(Files::isRegularFile)
                 .filter(p -> p.toString().endsWith(".json"))
                 .sorted(Comparator.comparing(p -> p.getFileName().toString())) // FIFO via timestamp prefix
                 .forEach(p -> files.add(p.toFile()));
        } catch (IOException e) {
            log.error("Failed to list pending files", e);
        }
        return files;
    }

    public QueueItem loadQueueItem(File file) throws IOException {
        return mapper.readValue(file, QueueItem.class);
    }

    public void moveToSuccess(File file) {
        moveFile(file, successDir.resolve(file.getName()));
    }

    public void moveToFailed(File file, QueueItem item) {
        try {
            item.setRetryCount(item.getRetryCount() + 1);
            item.setLastAttemptTime(java.time.LocalDateTime.now());
            mapper.writeValue(file, item); // Update metadata in place before moving
            moveFile(file, failedDir.resolve(file.getName()));
        } catch (IOException e) {
            log.error("Failed to update and move file to failed: " + file.getName(), e);
        }
    }

    private void moveFile(File source, Path target) {
        try {
            Files.move(source.toPath(), target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            log.info("Moved queue file {} to {}", source.getName(), target.getParent().getFileName());
        } catch (IOException e) {
            log.error("Failed to move file: " + source.getName(), e);
        }
    }

    @Data
    public static class QueueItem {
        private String table;
        private List<Map<String, Object>> data;
        private int retryCount;
        private java.time.LocalDateTime lastAttemptTime;
    }
}
