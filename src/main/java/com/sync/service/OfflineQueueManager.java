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

    private static final String QUEUE_DIR = "sync-queue";
    private final ObjectMapper mapper = new ObjectMapper();

    public OfflineQueueManager() {
        try {
            Files.createDirectories(Paths.get(QUEUE_DIR));
        } catch (IOException e) {
            log.error("Failed to create queue directory", e);
        }
    }

    public void saveQueue(String tableName, List<Map<String, Object>> data) {
        try {
            Path tableDir = Paths.get(QUEUE_DIR, tableName);
            Files.createDirectories(tableDir);

            String fileName = System.currentTimeMillis() + ".json";
            File file = tableDir.resolve(fileName).toFile();

            QueueItem item = new QueueItem();
            item.setTable(tableName);
            item.setData(data);
            item.setRetryCount(0);

            mapper.writeValue(file, item);
            log.info("Saved offline batch to queue: {}", file.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to save offline queue for table: " + tableName, e);
        }
    }

    public List<File> getAllQueuedFiles() {
        List<File> allFiles = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(Paths.get(QUEUE_DIR))) {
            paths.filter(Files::isRegularFile)
                 .filter(p -> p.toString().endsWith(".json"))
                 .forEach(p -> allFiles.add(p.toFile()));
        } catch (IOException e) {
            log.error("Failed to walk queue directory", e);
        }
        return allFiles;
    }

    public QueueItem loadQueueItem(File file) throws IOException {
        return mapper.readValue(file, QueueItem.class);
    }

    public void deleteQueue(File file) {
        if (file.delete()) {
            log.info("Deleted processed queue file: {}", file.getName());
        } else {
            log.warn("Failed to delete queue file: {}", file.getName());
        }
    }

    public void incrementRetryCount(File file, QueueItem item) {
        try {
            item.setRetryCount(item.getRetryCount() + 1);
            mapper.writeValue(file, item);
        } catch (IOException e) {
            log.error("Failed to update retry count for file: " + file.getName(), e);
        }
    }

    @Data
    public static class QueueItem {
        private String table;
        private List<Map<String, Object>> data;
        private int retryCount;
    }
}
