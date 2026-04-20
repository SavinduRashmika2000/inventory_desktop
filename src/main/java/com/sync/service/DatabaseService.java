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
        System.out.println(">>> DB: Fetching table names...");
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
        System.out.println(">>> DB: Found " + tables.size() + " tables.");
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
            System.err.println(">>> DB Error (PK) for " + tableName + ": " + e.getMessage());
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
        System.out.println(">>> DB: Checking change_log...");
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

    public int getTotalCount(String tableName) throws SQLException {
        String sql = String.format("SELECT count(*) FROM `%s` ", tableName);
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
        }
        return 0;
    }

    public int getUnsyncedCount(String tableName) throws SQLException {
        if (!hasCloudColumn(tableName)) return 0;
        String sql = String.format("SELECT count(*) FROM `%s` WHERE cloud = 0", tableName);
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
        }
        return 0;
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
                    Object value = rs.getObject(i);
                    if (value instanceof java.sql.Timestamp ts) {
                        value = ts.toInstant().toString();
                    }
                    row.put(columnName, value);
                }
                records.add(row);
            }
        }
        return records;
    }

    public void validateTableSchema(String tableName, List<Map<String, Object>> records) throws SQLException {
        if (records == null || records.isEmpty()) return;

        Set<String> dbColumns = getTableColumns(tableName);
        String pk = getPrimaryKeyColumn(tableName);

        if (pk == null) {
            throw new SQLException("Strict Validation Failed: No Primary Key found for table " + tableName);
        }

        for (Map<String, Object> record : records) {
            if (!record.containsKey(pk)) {
                throw new SQLException("Strict Validation Failed: Record missing primary key '" + pk + "' for table " + tableName);
            }
            for (String key : record.keySet()) {
                if (!dbColumns.contains(key)) {
                    throw new SQLException("Strict Validation Failed: Unknown column '" + key + "' in payload for table " + tableName);
                }
            }
        }
    }

    private Set<String> getTableColumns(String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Connection conn = getConnection();
             ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
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
