package com.sync;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URL;

public class AppLauncher extends Application {

    private Stage primaryStage;

    @Override
    public void start(Stage primaryStage) throws Exception {
        this.primaryStage = primaryStage;
        
        // Prevent application from exiting when all windows are closed
        Platform.setImplicitExit(false);
        
        // Initialize UI
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/sync/ui/sync-view.fxml"));
        Parent root = loader.load();
        
        Scene scene = new Scene(root, 900, 600);
        scene.getStylesheets().add(getClass().getResource("/com/sync/ui/style.css").toExternalForm());
        
        primaryStage.setTitle("Inventory Desktop Sync");
        primaryStage.getIcons().add(new Image(getClass().getResourceAsStream("/com/sync/ui/icon.png"))); // Placeholder icon
        primaryStage.setScene(scene);
        primaryStage.show();
        
        // Create System Tray
        createTrayIcon(primaryStage);
        
        primaryStage.setOnCloseRequest(event -> {
            primaryStage.hide();
            event.consume();
        });
    }

    private void createTrayIcon(Stage stage) {
        if (SystemTray.isSupported()) {
            SystemTray tray = SystemTray.getSystemTray();
            
            // To get a tray icon image, we normally load it. We'll use a placeholder or dummy.
            java.awt.Image image = Toolkit.getDefaultToolkit().createImage(getClass().getResource("/com/sync/ui/icon.png"));
            
            PopupMenu popup = new PopupMenu();
            
            MenuItem openItem = new MenuItem("Open Dashboard");
            openItem.addActionListener(e -> Platform.runLater(stage::show));
            
            MenuItem exitItem = new MenuItem("Exit");
            exitItem.addActionListener(e -> {
                Platform.exit();
                System.exit(0);
            });
            
            popup.add(openItem);
            popup.addSeparator();
            popup.add(exitItem);
            
            TrayIcon trayIcon = new TrayIcon(image, "Inventory Sync Service", popup);
            trayIcon.setImageAutoSize(true);
            trayIcon.addActionListener(e -> Platform.runLater(stage::show));
            
            try {
                tray.add(trayIcon);
            } catch (AWTException e) {
                System.err.println("TrayIcon could not be added.");
            }
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
