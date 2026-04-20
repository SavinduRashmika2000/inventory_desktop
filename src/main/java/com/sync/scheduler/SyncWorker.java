package com.sync.scheduler;

import com.sync.client.SyncApiClient;
import com.sync.dto.SyncStatusResponse;
import com.sync.exception.SyncException;
import com.sync.service.DatabaseService;
import com.sync.service.OfflineQueueManager;
import com.sync.service.SyncStateStore;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class SyncWorker implements Runnable {

    private final String tableName;
    private final DatabaseService dbService;
    private final SyncApiClient apiClient;
    private final OfflineQueueManager queueManager;
    private final SyncStateStore stateStore;
    private final Consumer<SyncProgress> progressListener;
    private final int batchSize;

    public SyncWorker(String tableName,
                      DatabaseService dbService,
                      SyncApiClient apiClient,
                      OfflineQueueManager queueManager,
                      SyncStateStore stateStore,
                      Consumer<SyncProgress> progressListener,
                      int batchSize) {
        this.tableName = tableName;
        this.dbService = dbService;
        this.apiClient = apiClient;
        this.queueManager = queueManager;
        this.stateStore = stateStore;
        this.progressListener = progressListener;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        Instant start = Instant.now();
        updateState(SyncStateStore.SyncState.SYNCING, null, null);
        log("Started sync for table: " + tableName);

        try {
            performSync();
            long duration = Duration.between(start, Instant.now()).toMillis();
            updateState(SyncStateStore.SyncState.IDLE, null, duration);
            log("Successfully completed sync for table: " + tableName);
        } catch (SyncException e) {
            long duration = Duration.between(start, Instant.now()).toMillis();
            updateState(SyncStateStore.SyncState.FAILED, e.getType().name() + ": " + e.getMessage(), duration);
            log("Sync failed for table " + tableName + ": " + e.getMessage());
        } catch (Exception e) {
            long duration = Duration.between(start, Instant.now()).toMillis();
            updateState(SyncStateStore.SyncState.FAILED, "UNKNOWN_ERROR: " + e.getMessage(), duration);
            log("Unexpected error syncing table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void performSync() {
        try {
            if (!dbService.hasCloudColumn(tableName)) {
                return;
            }

            String idColumn = dbService.getPrimaryKeyColumn(tableName);
            if (idColumn == null) {
                throw new SyncException("No Primary Key found", SyncException.ErrorType.VALIDATION_ERROR, tableName);
            }

            // --- Audit Phase ---
            notifyProgress(0, 0, "Auditing");
            int localTotal = dbService.getTotalCount(tableName);
            int localUnsynced = dbService.getUnsyncedCount(tableName);

            SyncStatusResponse cloudStatus = null;
            try {
                cloudStatus = apiClient.getSyncStatus(tableName);
            } catch (Exception e) {
                // Network error during status check
                throw new SyncException("Failed to fetch cloud status", SyncException.ErrorType.NETWORK_ERROR, tableName, e);
            }

            if (cloudStatus != null) {
                int cloudTotal = cloudStatus.getTotalRecords();
                if (localTotal == cloudTotal && localUnsynced == 0) {
                    notifyProgress(localTotal, localTotal, "Success");
                    return;
                }
            }

            // --- Sync Phase (Reconciliation) ---
            int totalProcessed = 0;
            while (true) {
                List<Map<String, Object>> batch = dbService.getUnsyncedDataBatch(tableName, batchSize);
                if (batch.isEmpty()) break;

                notifyProgress(totalProcessed, localTotal, "Pushing...");

                try {
                    apiClient.syncTable(tableName, batch);
                    
                    // Update local cloud flag
                    List<Object> ids = batch.stream().map(r -> r.get(idColumn)).toList();
                    dbService.updateCloudStatus(tableName, idColumn, ids);
                    
                    totalProcessed += batch.size();
                    notifyProgress(totalProcessed, localTotal, "Pushing...");
                } catch (SyncApiClient.ValidationException e) {
                    throw new SyncException("Server validation failed: " + e.getMessage(), SyncException.ErrorType.VALIDATION_ERROR, tableName, e);
                } catch (Exception e) {
                    // Fallback to offline queue if needed, or throw as network/server error
                    log("Network/Server error during batch push. Saving to offline queue for " + tableName);
                    queueManager.saveQueue(tableName, batch);
                    throw new SyncException("Push failed, moved to offline queue", SyncException.ErrorType.NETWORK_ERROR, tableName, e);
                }
            }

        } catch (SQLException e) {
            throw new SyncException("Database error: " + e.getMessage(), SyncException.ErrorType.DATABASE_ERROR, tableName, e);
        }
    }

    private void updateState(SyncStateStore.SyncState state, String error, Long duration) {
        stateStore.updateTableState(tableName, Instant.now().toString(), null);
        SyncStateStore.TableState tableState = stateStore.getTableState(tableName);
        tableState.state = state;
        tableState.lastError = error;
        if (duration != null) tableState.lastDurationMs = duration;
        // In a real app, you'd save the stateStore here
    }

    private void notifyProgress(int processed, int total, String status) {
        if (progressListener != null) {
            progressListener.accept(new SyncProgress(tableName, processed, total, status));
        }
    }

    private void log(String message) {
        // This will be replaced by a proper LogManager call later
        System.out.println("[" + tableName + "] " + message);
    }

    public record SyncProgress(String tableName, int processed, int total, String status) {}
}
