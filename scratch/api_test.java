import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApiTest {
    public static void main(String[] args) throws Exception {
        String url = "http://localhost:8080/api/sync/customer";
        ObjectMapper map = new ObjectMapper();
        
        List<Map<String, Object>> records = List.of(
            Map.of(
                "idcustomer", 25,
                "name", "Diagnostic Test",
                "tel", "0771234567",
                "email", "test@example.com",
                "status", 1,
                "type", 1,
                "dob", null,
                "address", "Test Address",
                "des", "Diagnostic Push",
                "cloud", 0
            )
        );

        String json = map.writeValueAsString(records);
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("Status: " + response.statusCode());
        System.out.println("Result: " + response.body());
    }
}
