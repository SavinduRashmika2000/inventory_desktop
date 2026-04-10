package com.sync.scheduler;

import com.sync.client.SyncApiClient;
import com.sync.dto.SyncResponse;
import com.sync.service.DatabaseService;
import com.sync.service.OfflineQueueManager;
import com.sync.service.SyncStateStore;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
public class SyncScheduler {

    private final DatabaseService dbService;
    private final SyncApiClient apiClient;
    private final OfflineQueueManager queueManager;
    private final SyncStateStore stateStore;
    
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> syncTask;
    private final long intervalMinutes;
    
    private Consumer<String> logListener;
    private Consumer<SyncProgress> progressListener;
    private boolean isSyncing = false;

    public SyncScheduler(DatabaseService dbService, 
                        SyncApiClient apiClient, 
                        OfflineQueueManager queueManager,
                        SyncStateStore stateStore,
                        long intervalMinutes) {
        this.dbService = dbService;
        this.apiClient = apiClient;
        this.queueManager = queueManager;
        this.stateStore = stateStore;
        this.intervalMinutes = intervalMinutes;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void setLogListener(Consumer<String> logListener) {
        this.logListener = logListener;
    }

    public void setProgressListener(Consumer<SyncProgress> progressListener) {
        this.progressListener = progressListener;
    }

    public void start() {
        if (syncTask != null && !syncTask.isDone()) {
            return;
        }
        syncTask = scheduler.scheduleAtFixedRate(this::runSyncCycle, 0, intervalMinutes, TimeUnit.MINUTES);
        log("Started automated sync cycle every " + intervalMinutes + " minute(s).");
    }

    public void stop() {
        if (syncTask != null) {
            syncTask.cancel(false);
            log("Stopped automated sync.");
        }
    }

    public void forceSync() {
        CompletableFuture.runAsync(this::runSyncCycle);
    }

    private void runSyncCycle() {
        if (isSyncing) {
            System.out.println(">>> Sync already in progress...");
            return;
        }

        if (!apiClient.isOnline()) {
            apiClient.checkConnection();
        }

        if (!apiClient.isOnline()) {
            System.out.println(">>> System Offline. Skipping cycle.");
            return;
        }
        
        isSyncing = true;
        System.out.println(">>> --- Starting Sync Cycle ---");
        try {
            // 1. Process Change Log (Deltas: Deletes, Updates, Inserts)
            System.out.println(">>> Part 1: Delta Changes...");
            syncDeltaChanges();

            // 2. Perform Full Reconciliation (Cloud=0 sweep)
            System.out.println(">>> Part 2: Reconciliation...");
            syncAllTables();
            
            System.out.println(">>> --- Sync Cycle Completed ---");
        } catch (Throwable t) {
            System.err.println(">>> CRITICAL ERROR in sync cycle: " + t.getClass().getName() + " - " + t.getMessage());
            t.printStackTrace();
        } finally {
            isSyncing = false;
        }
    }

    private void syncDeltaChanges() {
        System.out.println(">>> syncDeltaChanges() called...");
        int batchSize = 500;
        try {
            System.out.println(">>> Calling dbService.getPendingChanges()...");
            List<Map<String, Object>> changes = dbService.getPendingChanges(batchSize);
            System.out.println(">>> Pending changes count: " + (changes == null ? "null" : changes.size()));
            if (changes.isEmpty()) {
                return;
            }

            // Group changes by table and operation to optimize batching
            Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
            for (Map<String, Object> change : changes) {
                String key = change.get("table_name") + ":" + change.get("operation");
                grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(change);
            }

            for (Map.Entry<String, List<Map<String, Object>>> entry : grouped.entrySet()) {
                String[] parts = entry.getKey().split(":");
                String tableName = parts[0];
                String operation = parts[1];
                List<Map<String, Object>> logEntries = entry.getValue();

                log(String.format("Processing %d %s operations for %s", logEntries.size(), operation, tableName));

                if ("DELETE".equalsIgnoreCase(operation)) {
                    List<Object> ids = logEntries.stream().map(e -> e.get("record_id")).toList();
                    apiClient.deleteRecords(tableName, ids);
                } else {
                    // INSERT/UPDATE: We fetch the current state of the records
                    String pkColumn = dbService.getPrimaryKeyColumn(tableName);
                    List<Map<String, Object>> records = new ArrayList<>();
                    for (Map<String, Object> logEntry : logEntries) {
                        Map<String, Object> record = dbService.getRecordById(tableName, pkColumn, logEntry.get("record_id"));
                        if (record != null) records.add(record);
                    }
                    if (!records.isEmpty()) {
                        apiClient.syncTable(tableName, records);
                    }
                }

                // Mark these changes as synced locally
                List<Integer> changeIds = logEntries.stream().map(e -> (Integer) e.get("id")).toList();
                dbService.markChangesSynced(changeIds);
                log("Completed batch for " + tableName);
            }
        } catch (Exception e) {
            log("Error processing delta changes: " + e.getMessage());
        }
    }

    private void syncAllTables() {
        log(">>> Starting reconciliation sweep...");
        try {
            List<String> tables = dbService.getTableNames();
            log(">>> Found " + tables.size() + " tables in local database.");
            
            // Prioritize customer table and others that are critical for UI feedback
            tables.sort((a, b) -> {
                if (a.equalsIgnoreCase("customer")) return -1;
                if (b.equalsIgnoreCase("customer")) return 1;
                return a.compareTo(b);
            });

            // Immediately notify UI about all tables so they appear in the list
            for (String table : tables) {
                notifyProgress(table, 0, 0, "Discovered");
            }
            log(">>> Registered all tables for sync monitoring.");

            for (String table : tables) {
                if (table.equalsIgnoreCase("customer")) {
                    log(">>> [PRIORITY] Processing Customer table now...");
                }
                syncTable(table);
            }
            log(">>> Finished reconciliation sweep.");
        } catch (SQLException e) {
            log("!!! ERROR: Failed to discover tables: " + e.getMessage());
        }
    }

    private void syncTable(String tableName) {
        try {
            if (!dbService.hasCloudColumn(tableName)) {
                log("[" + tableName + "] Info: No cloud flag column. Skipping reconciliation.");
                return;
            }

            String idColumn = dbService.getPrimaryKeyColumn(tableName);
            if (idColumn == null) {
                log("[" + tableName + "] Warn: No Primary Key found. Skipping reconciliation.");
                return;
            }

            int batchSize = 500;
            int totalProcessed = 0;
            
            while (true) {
                List<Map<String, Object>> batch = dbService.getUnsyncedDataBatch(tableName, batchSize);
                if (batch.isEmpty()) {
                    if (totalProcessed == 0) {
                        notifyProgress(tableName, 0, 0, "Idle");
                    }
                    break;
                }

                log("[" + tableName + "] Reconciling " + batch.size() + " unsynced records...");
                notifyProgress(tableName, totalProcessed, totalProcessed + batch.size(), "Pushing...");

                try {
                    apiClient.syncTable(tableName, batch);
                    
                    // Update local cloud flag
                    List<Object> ids = batch.stream().map(r -> r.get(idColumn)).toList();
                    dbService.updateCloudStatus(tableName, idColumn, ids);
                    
                    totalProcessed += batch.size();
                    log("[" + tableName + "] Successfully pushed " + batch.size() + " records. (Total: " + totalProcessed + ")");
                    notifyProgress(tableName, totalProcessed, totalProcessed, "Success");
                    
                    if (batch.size() < batchSize) break;
                } catch (Exception e) {
                    log("[" + tableName + "] Sync Error: " + e.getMessage());
                    notifyProgress(tableName, totalProcessed, totalProcessed + batch.size(), "Error");
                    break;
                }
            }
            
        } catch (Exception e) {
            log("[" + tableName + "] Critical System Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void log(String message) {
        log.info(message);
        if (logListener != null) {
            logListener.accept(message);
        }
    }

    private void notifyProgress(String tableName, int processed, int total, String status) {
        if (progressListener != null) {
            progressListener.accept(new SyncProgress(tableName, processed, total, status));
        }
    }

    public record SyncProgress(String tableName, int processed, int total, String status) {}

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }

}
