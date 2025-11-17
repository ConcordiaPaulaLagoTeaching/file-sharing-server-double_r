package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileServer {

    private FileSystemManager fsManager;
    private int port;
    private ExecutorService threadPool;

    public FileServer(int port, String fileSystemName, int totalSize) {
        try {
            // This now correctly uses the 'totalSize' parameter from Main.java
            this.fsManager = new FileSystemManager(fileSystemName, totalSize);
            this.port = port;
            // Use cached thread pool for better scalability
            this.threadPool = Executors.newCachedThreadPool();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize FileSystemManager", e);
        }
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started. Listening on port " + port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Submit to thread pool instead of creating new thread each time
                threadPool.submit(() -> handleClient(clientSocket));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        } finally {
            if (threadPool != null) {
                threadPool.shutdown();
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    String response = processCommand(line);
                    writer.println(response);
                    
                    // Disconnect on QUIT command
                    if ("SUCCESS: Disconnecting.".equals(response)) {
                        break;
                    }
                } catch (Exception e) {
                    writer.println("ERROR: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            // Client disconnected or error - just close silently
        } finally {
            try {
                clientSocket.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    private String processCommand(String line) throws Exception {
        if (line == null || line.trim().isEmpty()) {
            return "ERROR: Empty command";
        }

        // Split into 3 parts max: [COMMAND, FILENAME, CONTENT]
        String[] parts = line.trim().split("\\s+", 3);
        if (parts.length == 0) {
            return "ERROR: Empty command";
        }

        String command = parts[0].toUpperCase();

        switch (command) {
            case "CREATE":
                if (parts.length < 2) {
                    return "ERROR: CREATE requires a filename";
                }
                return handleCreate(parts[1]);

            case "WRITE":
                if (parts.length < 3) {
                    return "ERROR: WRITE requires filename and content";
                }
                return handleWrite(parts[1], parts[2]);

            case "READ":
                if (parts.length < 2) {
                    return "ERROR: READ requires a filename";
                }
                return handleRead(parts[1]);

            case "DELETE":
                if (parts.length < 2) {
                    return "ERROR: DELETE requires a filename";
                }
                return handleDelete(parts[1]);

            case "LIST":
                return handleList();

            case "QUIT":
                return "SUCCESS: Disconnecting.";

            default:
                return "ERROR: Unknown command.";
        }
    }

    // --- Command Handler Methods ---

    private String handleCreate(String filename) {
        try {
            // fsManager handles all logic, including "filename too long"
            // and "file already exists" (by returning silently)
            fsManager.createFile(filename);
            return "SUCCESS: File '" + filename + "' created.";
        } catch (Exception e) {
            // Return the specific error message from fsManager
            return e.getMessage(); 
        }
    }

    private String handleWrite(String filename, String content) {
        try {
            fsManager.writeFile(filename, content.getBytes());
            return "SUCCESS: File '" + filename + "' written.";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private String handleRead(String filename) {
        try {
            byte[] data = fsManager.readFile(filename);
            // The test expects the data, not "SUCCESS: data"
            return new String(data);
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private String handleDelete(String filename) {
        try {
            fsManager.deleteFile(filename);
            return "SUCCESS: File '" + filename + "' deleted.";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private String handleList() {
        try {
            String[] files = fsManager.listFiles();
            if (files.length == 0) {
                return "No files in filesystem.";
            }
            // ClientRunner expects a single-line response.
            // Using ", " as a delimiter is safer.
            return String.join(", ", files);
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }
}