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

    private static final String STATE_FILE = "sync-state.json";
    private final ObjectMapper mapper = new ObjectMapper();
    private Map<String, String> syncStates = new HashMap<>();

    public SyncStateStore() {
        load();
    }

    public synchronized String getLastSyncTime(String tableName) {
        return syncStates.get(tableName);
    }

    public synchronized void updateLastSyncTime(String tableName, String timestamp) {
        syncStates.put(tableName, timestamp);
        save();
    }

    private void load() {
        File file = new File(STATE_FILE);
        if (file.exists()) {
            try {
                syncStates = mapper.readValue(file, new TypeReference<Map<String, String>>() {});
                log.info("Loaded sync states for {} tables", syncStates.size());
            } catch (IOException e) {
                log.error("Failed to load sync state file", e);
            }
        }
    }

    private void save() {
        try {
            mapper.writeValue(new File(STATE_FILE), syncStates);
        } catch (IOException e) {
            log.error("Failed to save sync state file", e);
        }
    }
}
