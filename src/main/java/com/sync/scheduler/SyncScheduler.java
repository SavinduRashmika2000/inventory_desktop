package com.sync.scheduler;

import com.sync.client.SyncApiClient;
import com.sync.dto.SyncResponse;
import com.sync.service.DatabaseService;
import com.sync.service.LocalLogManager;
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
    private final LocalLogManager logManager;
    
    private final ScheduledExecutorService scheduler;
    private final ExecutorService syncThreadPool;
    private final Set<String> activeSyncs = Collections.newSetFromMap(new ConcurrentHashMap<>());
    
    private ScheduledFuture<?> syncTask;
    private final long intervalMinutes;
    private final int batchSize;
    private final int maxConcurrency;
    
    private Consumer<String> logListener;
    private Consumer<SyncWorker.SyncProgress> progressListener;
    private boolean isCycleRunning = false;

    public boolean isOnline() {
        return apiClient != null && apiClient.isOnline();
    }

    public SyncScheduler(DatabaseService dbService, 
                        SyncApiClient apiClient, 
                        OfflineQueueManager queueManager,
                        SyncStateStore stateStore,
                        LocalLogManager logManager,
                        long intervalMinutes,
                        int batchSize,
                        int maxConcurrency) {
        this.dbService = dbService;
        this.apiClient = apiClient;
        this.queueManager = queueManager;
        this.stateStore = stateStore;
        this.logManager = logManager;
        this.intervalMinutes = intervalMinutes;
        this.batchSize = batchSize;
        this.maxConcurrency = maxConcurrency;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.syncThreadPool = Executors.newFixedThreadPool(maxConcurrency);
    }

    public void setLogListener(Consumer<String> logListener) {
        this.logListener = logListener;
    }

    public void setProgressListener(Consumer<SyncWorker.SyncProgress> progressListener) {
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
        if (isCycleRunning) {
            log("Sync cycle already in progress, skipping.");
            return;
        }

        if (!apiClient.isOnline()) {
            apiClient.checkConnection();
        }

        if (!apiClient.isOnline()) {
            log("System Offline. Skipping sync cycle.");
            return;
        }
        
        isCycleRunning = true;
        log("--- Starting Sync Cycle ---");
        try {
            // 1. Delta Changes (Sequential or specialized task)
            syncDeltaChanges();

            // 2. Full Reconciliation (Concurrent via ThreadPool)
            List<String> tables = dbService.getTableNames();
            for (String tableName : tables) {
                if (activeSyncs.contains(tableName)) {
                    log("Sync already active for " + tableName + ", skipping in this cycle.");
                    continue;
                }

                // Submit to thread pool
                activeSyncs.add(tableName);
                syncThreadPool.submit(() -> {
                    try {
                        new SyncWorker(tableName, dbService, apiClient, queueManager, stateStore, 
                                       logManager, progressListener, batchSize).run();
                    } finally {
                        activeSyncs.remove(tableName);
                    }
                });
            }
        } catch (Exception e) {
            log("CRITICAL ERROR in sync cycle: " + e.getMessage());
            e.printStackTrace();
        } finally {
            isCycleRunning = false;
        }
    }

    private void syncDeltaChanges() {
        try {
            List<Map<String, Object>> changes = dbService.getPendingChanges(batchSize);
            if (changes.isEmpty()) return;

            log("Processing " + changes.size() + " delta changes...");
            
            // Group by table for batching efficacy
            Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
            for (Map<String, Object> change : changes) {
                String key = (String) change.get("table_name") + ":" + change.get("operation");
                grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(change);
            }

            for (Map.Entry<String, List<Map<String, Object>>> entry : grouped.entrySet()) {
                String[] parts = entry.getKey().split(":");
                String tableName = parts[0];
                String operation = parts[1];
                List<Map<String, Object>> logEntries = entry.getValue();

                if ("DELETE".equalsIgnoreCase(operation)) {
                    List<Object> ids = logEntries.stream().map(e -> e.get("record_id")).toList();
                    apiClient.deleteRecords(tableName, ids);
                } else {
                    String pkColumn = dbService.getPrimaryKeyColumn(tableName);
                    List<Map<String, Object>> records = new ArrayList<>();
                    for (Map<String, Object> logEntry : logEntries) {
                        Map<String, Object> record = dbService.getRecordById(tableName, pkColumn, logEntry.get("record_id"));
                        if (record != null) records.add(record);
                    }
                    if (!records.isEmpty()) apiClient.syncTable(tableName, records);
                }

                dbService.markChangesSynced(logEntries.stream().map(e -> (Integer) e.get("id")).toList());
            }
            log("Delta changes sync completed.");
        } catch (Exception e) {
            log("Error processing delta changes: " + e.getMessage());
        }
    }

    private void log(String message) {
        log.info(message);
        if (logManager != null) {
            logManager.logGlobal(message);
        }
        if (logListener != null) {
            logListener.accept(message);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        syncThreadPool.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) scheduler.shutdownNow();
            if (!syncThreadPool.awaitTermination(30, TimeUnit.SECONDS)) syncThreadPool.shutdownNow();
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            syncThreadPool.shutdownNow();
        }
    }

    public record SyncProgress(String tableName, int processed, int total, String status) {}
}
