package com.sync.ui;

import com.sync.client.SyncApiClient;
import com.sync.scheduler.SyncScheduler;
import com.sync.service.DatabaseService;
import com.sync.service.OfflineQueueManager;
import com.sync.service.SyncStateStore;
import javafx.application.Platform;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class MainController {

    @FXML private TableView<TableSyncInfo> tablesTable;
    @FXML private TableColumn<TableSyncInfo, String> tableNameCol;
    @FXML private TableColumn<TableSyncInfo, Integer> processedCol;
    @FXML private TableColumn<TableSyncInfo, Integer> totalCol;
    @FXML private TableColumn<TableSyncInfo, String> statusCol;
    @FXML private TextArea logArea;
    @FXML private Label statusLabel;
    @FXML private Button startBtn;
    @FXML private Button stopBtn;

    private SyncScheduler scheduler;
    private final ObservableList<TableSyncInfo> tableList = FXCollections.observableArrayList();

    @FXML
    public void initialize() {
        setupTable();
        loadConfiguration();
        handleStart(); // Automatic production start
    }

    private void setupTable() {
        tableNameCol.setCellValueFactory(new PropertyValueFactory<>("tableName"));
        processedCol.setCellValueFactory(new PropertyValueFactory<>("processed"));
        totalCol.setCellValueFactory(new PropertyValueFactory<>("total"));
        statusCol.setCellValueFactory(new PropertyValueFactory<>("status"));
        
        tablesTable.setItems(tableList);
    }

    private void loadConfiguration() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
            
            DatabaseService dbService = new DatabaseService(
                props.getProperty("db.host"),
                props.getProperty("db.port"),
                props.getProperty("db.name"),
                props.getProperty("db.user"),
                props.getProperty("db.password")
            );
            
            SyncApiClient apiClient = new SyncApiClient(props.getProperty("api.base.url"));
            OfflineQueueManager queueManager = new OfflineQueueManager();
            SyncStateStore stateStore = new SyncStateStore();
            
            long interval = Long.parseLong(props.getProperty("sync.interval.minutes", "1"));
            
            scheduler = new SyncScheduler(dbService, apiClient, queueManager, stateStore, interval);
            scheduler.setLogListener(this::appendLog);
            scheduler.setProgressListener(this::updateProgress);
            
            appendLog("System initialized in Production Mode.");
            
        } catch (Exception e) {
            appendLog("Bootstrap Error: " + e.getMessage());
            statusLabel.setText("System Offline");
        }
    }

    @FXML
    private void handleStart() {
        if (scheduler != null) {
            scheduler.start();
            startBtn.setDisable(true);
            stopBtn.setDisable(false);
            statusLabel.setText("Active / Online");
        }
    }

    @FXML
    private void handleStop() {
        if (scheduler != null) {
            scheduler.stop();
            startBtn.setDisable(false);
            stopBtn.setDisable(true);
            statusLabel.setText("Standby / Paused");
        }
    }

    @FXML
    private void handleForceSync() {
        if (scheduler != null) {
            appendLog("Forcing immediate check for updates...");
            scheduler.forceSync();
        }
    }

    private void appendLog(String message) {
        Platform.runLater(() -> {
            String timestamp = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            logArea.appendText("[" + timestamp + "] " + message + "\n");
        });
    }

    private void updateProgress(SyncScheduler.SyncProgress progress) {
        Platform.runLater(() -> {
            TableSyncInfo info = findTableInfo(progress.tableName());
            if (info == null) {
                info = new TableSyncInfo(progress.tableName());
                tableList.add(info);
            }
            info.setProcessed(progress.processed());
            info.setTotal(progress.total());
            
            String displayStatus = switch (progress.status()) {
                case "Discovered" -> "🔍 Discovered";
                case "Auditing" -> "⚖️ Checking Parity";
                case "Pushing..." -> "🚀 Pushing Data";
                case "Success" -> "✨ Sync Complete";
                case "Idle" -> "✅ Up to Date";
                case "Error" -> "❌ Sync Error";
                case "Invalid Data" -> "⚠️ Validation Error";
                default -> progress.status();
            };
            info.setStatus(displayStatus);
            
            // Mirror online status in header
            // (Assuming SyncScheduler provides an isOnline state in an improved version)
            tablesTable.refresh();
        });
    }

    private TableSyncInfo findTableInfo(String name) {
        return tableList.stream().filter(t -> t.getTableName().equals(name)).findFirst().orElse(null);
    }

    @Data
    public static class TableSyncInfo {
        private final SimpleStringProperty tableName;
        private final SimpleIntegerProperty processed;
        private final SimpleIntegerProperty total;
        private final SimpleStringProperty status;

        public TableSyncInfo(String name) {
            this.tableName = new SimpleStringProperty(name);
            this.processed = new SimpleIntegerProperty(0);
            this.total = new SimpleIntegerProperty(0);
            this.status = new SimpleStringProperty("Idle");
        }

        public String getTableName() { return tableName.get(); }
        public int getProcessed() { return processed.get(); }
        public void setProcessed(int v) { processed.set(v); }
        public int getTotal() { return total.get(); }
        public void setTotal(int v) { total.set(v); }
        public String getStatus() { return status.get(); }
        public void setStatus(String v) { status.set(v); }
    }
}
