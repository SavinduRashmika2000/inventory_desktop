package com.sync.service;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class LocalLogManager {

    private final String logDir;
    private final DateTimeFormatter fileFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final DateTimeFormatter logFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    public LocalLogManager(String logDir) {
        this.logDir = logDir;
        try {
            Files.createDirectories(Paths.get(logDir));
        } catch (IOException e) {
            log.error("Failed to create log directory", e);
        }
    }

    public synchronized void log(String tableName, String message) {
        String fileName = String.format("sync_%s.log", LocalDate.now().format(fileFormatter));
        Path logPath = Paths.get(logDir, fileName);

        String timestamp = LocalDateTime.now().format(logFormatter);
        String formattedMessage = String.format("[%s] [%s] %s\n", timestamp, tableName != null ? tableName : "GLOBAL", message);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logPath.toFile(), true))) {
            writer.write(formattedMessage);
        } catch (IOException e) {
            log.error("Failed to write to local log file", e);
        }
    }

    public void logGlobal(String message) {
        log(null, message);
    }
}
