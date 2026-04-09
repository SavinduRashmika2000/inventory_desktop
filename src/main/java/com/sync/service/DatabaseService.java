package com.sync.service;

import lombok.extern.slf4j.Slf4j;
import java.sql.*;
import java.util.*;

@Slf4j
public class DatabaseService {

    private final String url;
    private final String user;
    private final String password;

    public DatabaseService(String host, String port, String dbName, String user, String password) {
        this.url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL", host, port, dbName);
        this.user = user;
        this.password = password;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public List<String> getTableNames() throws SQLException {
        List<String> tables = new ArrayList<>();
        // Prefer direct query for speed and reliability in MySQL
        String sql = "SHOW TABLES";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    public String getPrimaryKeyColumn(String tableName) {
        String sql = "SHOW KEYS FROM `" + tableName + "` WHERE Key_name = 'PRIMARY'";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();
            if (rs.next()) {
                // Find column by name "Column_name" case-insensitively
                for (int i = 1; i <= colCount; i++) {
                    if ("Column_name".equalsIgnoreCase(rsmd.getColumnName(i))) {
                        return rs.getString(i);
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Could not find primary key for {}: {}", tableName, e.getMessage());
        }
        return null;
    }

    public boolean hasCloudColumn(String tableName) throws SQLException {
        String sql = "DESCRIBE `" + tableName + "`";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    if ("Field".equalsIgnoreCase(rsmd.getColumnName(i))) {
                        if ("cloud".equalsIgnoreCase(rs.getString(i))) return true;
                    }
                }
            }
        }
        return false;
    }

    public List<Map<String, Object>> getUnsyncedDataBatch(String tableName, int limit) throws SQLException {
        if (!hasCloudColumn(tableName)) return Collections.emptyList();
        
        String sql = String.format("SELECT * FROM `%s` WHERE cloud = 0 LIMIT %d", tableName, limit);
        return executeQueryToMap(sql);
    }

    public List<Map<String, Object>> getPendingChanges(int limit) throws SQLException {
        String sql = String.format("SELECT * FROM change_log WHERE synced = 0 LIMIT %d", limit);
        return executeQueryToMap(sql);
    }

    public void updateCloudStatus(String tableName, String idColumn, List<Object> ids) throws SQLException {
        if (ids.isEmpty()) return;
        String placeholders = ids.stream().map(id -> "?").reduce((a, b) -> a + "," + b).get();
        String sql = String.format("UPDATE `%s` SET cloud = 1 WHERE `%s` IN (%s)", tableName, idColumn, placeholders);
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                stmt.setObject(i + 1, ids.get(i));
            }
            stmt.executeUpdate();
        }
    }

    public void markChangesSynced(List<Integer> changeLogIds) throws SQLException {
        if (changeLogIds.isEmpty()) return;
        String placeholders = changeLogIds.stream().map(id -> "?").reduce((a, b) -> a + "," + b).get();
        String sql = String.format("UPDATE change_log SET synced = 1 WHERE id IN (%s)", placeholders);
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < changeLogIds.size(); i++) {
                stmt.setInt(i + 1, changeLogIds.get(i));
            }
            stmt.executeUpdate();
        }
    }

    private List<Map<String, Object>> executeQueryToMap(String sql) throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = null;
                    try {
                        value = rs.getObject(i);
                        
                        if (value instanceof java.sql.Timestamp ts) {
                            value = ts.toInstant().toString();
                        } else if (value instanceof java.sql.Date d) {
                            value = d.toString();
                        } else if (value instanceof java.sql.Time t) {
                            value = t.toString();
                        }
                    } catch (Exception e) {
                        log.warn("Error reading column {} in {}: {}", columnName, sql, e.getMessage());
                        // Fallback: try to read as string if object-mapping fails
                        try { value = rs.getString(i); } catch (Exception ignored) {}
                    }
                    row.put(columnName, value);
                }
                records.add(row);
            }
        }
        return records;
    }

    /**
     * Fetches a single record by its table and ID. Used for Change Log processing.
     */
    public Map<String, Object> getRecordById(String tableName, String pkColumn, Object recordId) throws SQLException {
        String sql = String.format("SELECT * FROM `%s` WHERE `%s` = ?", tableName, pkColumn);
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, recordId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        Object value = rs.getObject(i);
                        if (value instanceof java.sql.Timestamp ts) value = ts.toInstant().toString();
                        row.put(metaData.getColumnName(i), value);
                    }
                    return row;
                }
            }
        }
        return null;
    }


}
