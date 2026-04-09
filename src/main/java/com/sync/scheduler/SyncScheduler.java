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
import java.util.List;
import java.util.Map;
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
            log("Sync cycle already in progress, skipping...");
            return;
        }
        
        isSyncing = true;
        try {
            // 1. Process Offline Queue First
            processOfflineQueue();

            // 2. Process New Data Sync
            syncAllTables();
            
        } catch (Exception e) {
            log("Critical error in sync cycle: " + e.getMessage());
        } finally {
            isSyncing = false;
        }
    }

    private void processOfflineQueue() {
        List<File> queuedFiles = queueManager.getAllQueuedFiles();
        if (queuedFiles.isEmpty()) return;

        log("Found " + queuedFiles.size() + " queued batches. Attempting restoration...");
        for (File file : queuedFiles) {
            if (!apiClient.isOnline()) {
                log("System still offline. Skipping queue restoration.");
                break;
            }

            try {
                OfflineQueueManager.QueueItem item = queueManager.loadQueueItem(file);
                log("Retrying queued batch for table: " + item.getTable() (Attempt " + (item.getRetryCount() + 1) + ")");
                
                apiClient.syncTable(item.getTable(), item.getData());
                queueManager.deleteQueue(file);
                log("Successfully restored queued batch: " + file.getName());
            } catch (SyncApiClient.ValidationException e) {
                log("Validation error in queued batch. Discarding: " + e.getMessage());
                queueManager.deleteQueue(file);
            } catch (Exception e) {
                log("Retry failed for batch " + file.getName() + ": " + e.getMessage());
                // Logic to increment retry count could go here if item was re-loaded
            }
        }
    }

    private void syncAllTables() {
        log("Checking for new data updates...");
        try {
            List<String> tables = dbService.getTableNames();
            for (String table : tables) {
                syncTable(table);
            }
        } catch (SQLException e) {
            log("Failed to discover tables: " + e.getMessage());
        }
    }

    private void syncTable(String tableName) {
        try {
            String initialLastSync = stateStore.getLastSyncTime(tableName);
            int batchSize = 500;
            int offset = 0;
            int totalProcessed = 0;
            String latestTimestampInCycle = initialLastSync;
            boolean hadError = false;
            
            while (true) {
                List<Map<String, Object>> batch = dbService.getTableDataBatch(tableName, initialLastSync, batchSize, offset);
                if (batch.isEmpty()) {
                    if (totalProcessed == 0) {
                        notifyProgress(tableName, 0, 0, "Up to Date");
                    }
                    break;
                }

                log("Syncing " + batch.size() + " records for table: " + tableName + " (Total so far: " + totalProcessed + ")");
                notifyProgress(tableName, totalProcessed, totalProcessed + batch.size(), "Processing...");

                try {
                    apiClient.syncTable(tableName, batch);
                    
                    String batchLatest = getLatestTimestamp(batch);
                    if (batchLatest != null && (latestTimestampInCycle == null || batchLatest.compareTo(latestTimestampInCycle) > 0)) {
                        latestTimestampInCycle = batchLatest;
                    }
                    
                    totalProcessed += batch.size();
                    log("[" + tableName + "] Successfully synced batch. Total synced so far: " + totalProcessed);
                    notifyProgress(tableName, totalProcessed, totalProcessed, "Batch Success");
                    
                    if (batch.size() < batchSize) {
                        break; // No more records to fetch
                    }
                    offset += batchSize;
                } catch (SyncApiClient.ValidationException e) {
                    hadError = true;
                    log("[" + tableName + "] Validation error: " + e.getMessage() + ". Skipping batch.");
                    notifyProgress(tableName, totalProcessed, totalProcessed + batch.size(), "Invalid Data");
                    break;
                } catch (Exception e) {
                    hadError = true;
                    if (!apiClient.isOnline()) {
                        queueManager.saveQueue(tableName, batch);
                        log("[" + tableName + "] System went offline. Batch moved to local queue.");
                        notifyProgress(tableName, totalProcessed, totalProcessed + batch.size(), "Offline (Queued)");
                    } else {
                        log("[" + tableName + "] API Error: " + e.getMessage());
                        notifyProgress(tableName, totalProcessed, totalProcessed + batch.size(), "API Error");
                    }
                    break;
                }
            }
            
            // Only update lastSyncTime if we fetched and synced at least something
            if (latestTimestampInCycle != null && !latestTimestampInCycle.equals(initialLastSync)) {
                stateStore.updateLastSyncTime(tableName, latestTimestampInCycle);
            }
            
            // At the end of a successful full-table batch loop, ask the server for status
            if (totalProcessed > 0 && !hadError) {
                 try {
                     com.sync.dto.SyncStatusResponse status = apiClient.getSyncStatus(tableName);
                     if (status != null) {
                         log("[" + tableName + "] Sync Complete. Cloud total items: " + status.getTotalRecords() + " (Last Sync: " + status.getLastSyncTimestamp() + ")");
                         notifyProgress(tableName, totalProcessed, totalProcessed, "Success");
                     }
                 } catch (Exception e) {
                     log("[" + tableName + "] Failed to get sync status: " + e.getMessage());
                     notifyProgress(tableName, totalProcessed, totalProcessed, "Success");
                 }
            }
            
        } catch (Exception e) {
            log("[" + tableName + "] Processing error: " + e.getMessage());
        }
    }

    private String getLatestTimestamp(List<Map<String, Object>> batch) {
        return batch.stream()
                .filter(row -> row.containsKey("updated_at"))
                .map(row -> (String) row.get("updated_at"))
                .max(String::compareTo)
                .orElse(null);
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
