package com.sync.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SyncStateStore {

    public enum SyncState {
        IDLE, SYNCING, FAILED, QUEUED
    }

    public static class TableState {
        public String tableName;
        public String lastSyncTime;
        public Integer lastCloudCount;
        public Long lastDurationMs;
        public String lastError;
        public SyncState state = SyncState.IDLE;
        public Integer totalRecordsSynced = 0;
    }

    private static final String STATE_FILE = "sync-state.json";
    private final ObjectMapper mapper = new ObjectMapper();
    private Map<String, TableState> tableStates = new HashMap<>();

    public SyncStateStore() {
        load();
    }

    public synchronized TableState getTableState(String tableName) {
        return tableStates.getOrDefault(tableName, new TableState());
    }

    public synchronized void updateTableState(String tableName, String timestamp, Integer cloudCount) {
        TableState state = tableStates.getOrDefault(tableName, new TableState());
        if (timestamp != null) state.lastSyncTime = timestamp;
        if (cloudCount != null) state.lastCloudCount = cloudCount;
        tableStates.put(tableName, state);
        save();
    }

    private void load() {
        File file = new File(STATE_FILE);
        if (file.exists()) {
            try {
                tableStates = mapper.readValue(file, new TypeReference<Map<String, TableState>>() {});
                log.info("Loaded sync states for {} tables", tableStates.size());
            } catch (IOException e) {
                log.error("Failed to load sync state file", e);
            }
        }
    }

    private void save() {
        try {
            mapper.writeValue(new File(STATE_FILE), tableStates);
        } catch (IOException e) {
            log.error("Failed to save sync state file", e);
        }
    }
}
