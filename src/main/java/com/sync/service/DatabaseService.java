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
        this.url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", host, port, dbName);
        this.user = user;
        this.password = password;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public List<String> getTableNames() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    // System schema patterns for exclusion
                    String schema = rs.getString("TABLE_SCHEM");
                    if (schema == null || (!schema.equalsIgnoreCase("information_schema") && 
                                          !schema.equalsIgnoreCase("mysql") && 
                                          !schema.equalsIgnoreCase("performance_schema") && 
                                          !schema.equalsIgnoreCase("sys"))) {
                        tables.add(tableName);
                    }
                }
            }
        }
        return tables;
    }

    public boolean hasUpdatedAtColumn(String tableName) throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getColumns(null, null, tableName, "updated_at")) {
                return rs.next();
            }
        }
    }

    public List<Map<String, Object>> getTableDataBatch(String tableName, String lastSyncTime, int limit, int offset) throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();
        boolean incremental = hasUpdatedAtColumn(tableName) && lastSyncTime != null;
        
        String sql;
        if (incremental) {
            sql = String.format("SELECT * FROM %s WHERE updated_at > ? ORDER BY updated_at LIMIT %d OFFSET %d", tableName, limit, offset);
        } else {
            sql = String.format("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, limit, offset);
        }
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            if (incremental) {
                stmt.setString(1, lastSyncTime);
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        // Convert java.sql.Timestamp to String for JSON if not handled by Jackson
                        if (value instanceof java.sql.Timestamp ts) {
                            value = ts.toInstant().toString();
                        }
                        row.put(columnName, value);
                    }
                    records.add(row);
                }
            }
        }
        return records;
    }
}
