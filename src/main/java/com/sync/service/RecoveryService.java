package com.sync.service;

import com.sync.client.SyncApiClient;
import com.sync.scheduler.SyncWorker;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class RecoveryService {

    private final OfflineQueueManager queueManager;
    private final SyncApiClient apiClient;
    private final SyncStateStore stateStore;
    private final ExecutorService recoveryExecutor = Executors.newSingleThreadExecutor();

    public RecoveryService(OfflineQueueManager queueManager, 
                           SyncApiClient apiClient, 
                           SyncStateStore stateStore) {
        this.queueManager = queueManager;
        this.apiClient = apiClient;
        this.stateStore = stateStore;
    }

    public void runRecovery() {
        recoveryExecutor.submit(() -> {
            log.info("Starting crash recovery scan...");
            List<File> pendingFiles = queueManager.getPendingFiles();
            
            if (pendingFiles.isEmpty()) {
                log.info("No pending sync operations found during recovery.");
                return;
            }

            log.info("Found {} pending operations. Resuing sync...", pendingFiles.size());
            
            for (File file : pendingFiles) {
                try {
                    OfflineQueueManager.QueueItem item = queueManager.loadQueueItem(file);
                    log.info("Attempting recovery for table: {}", item.getTable());
                    
                    try {
                        apiClient.syncTable(item.getTable(), item.getData());
                        queueManager.moveToSuccess(file);
                        log.info("Successfully recovered batch for {}", item.getTable());
                    } catch (Exception e) {
                        log.warn("Recovery attempt failed for {}: {}. Moving to failed folder.", item.getTable(), e.getMessage());
                        queueManager.moveToFailed(file, item);
                    }
                } catch (IOException e) {
                    log.error("Failed to load queue item for recovery: " + file.getName(), e);
                }
            }
            log.info("Crash recovery scan completed.");
        });
    }

    public void shutdown() {
        recoveryExecutor.shutdown();
    }
}
