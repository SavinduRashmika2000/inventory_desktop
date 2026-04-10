import java.sql.*;
import java.util.*;

public class db_diag {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/itmind_inventory_desktop?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL";
        String user = "root";
        String password = "";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connection successful!");
            
            // Test 1: List Tables
            System.out.println("\n--- Testing Table Discovery ---");
            List<String> tables = new ArrayList<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            System.out.println("Found " + tables.size() + " tables.");
            if (tables.contains("customer")) {
                System.out.println("SUCCESS: 'customer' table found.");
            } else {
                System.out.println("FAILURE: 'customer' table NOT found.");
            }

            // Test 2: Cloud Column Check
            System.out.println("\n--- Testing Cloud Column Check (customer) ---");
            boolean hasCloud = false;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("DESCRIBE customer")) {
                while (rs.next()) {
                    if ("cloud".equalsIgnoreCase(rs.getString("Field"))) {
                        hasCloud = true;
                    }
                }
            }
            System.out.println("Customer has cloud column: " + hasCloud);

            // Test 3: Unsynced Data Fetch (customer 25)
            System.out.println("\n--- Testing Fetch for idcustomer = 25 ---");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM customer WHERE idcustomer = 25")) {
                if (rs.next()) {
                    ResultSetMetaData md = rs.getMetaData();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        System.out.println(md.getColumnName(i) + ": " + rs.getObject(i));
                    }
                } else {
                    System.out.println("ID 25 NOT FOUND in local DB.");
                }
            }

            // Test 4: Change Log Check
            System.out.println("\n--- Testing Change Log Check ---");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM change_log WHERE synced = 0")) {
                if (rs.next()) {
                    System.out.println("Pending changes: " + rs.getInt(1));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
