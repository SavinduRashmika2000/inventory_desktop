import java.sql.*;
import java.util.*;

public class db_test {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://localhost:3306/itmind_inventory?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        String user = "root";
        String password = "";
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables("itmind_inventory", null, "%", new String[]{"TABLE"})) {
                System.out.println("Tables found:");
                boolean foundCustomer = false;
                int count = 0;
                while (rs.next()) {
                    String name = rs.getString("TABLE_NAME");
                    count++;
                    if (name.equalsIgnoreCase("customer")) {
                        foundCustomer = true;
                        System.out.println("FOUND: " + name);
                    }
                }
                System.out.println("Total tables: " + count);
                System.out.println("Customer present: " + foundCustomer);
            }
        }
    }
}
