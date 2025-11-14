package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Handles communication with a single client in its own thread.
 */
public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final FileSystemManager fsManager;

    public ClientHandler(Socket socket, FileSystemManager manager) {
        this.clientSocket = socket;
        this.fsManager = manager; // Shared, synchronized resource
    }

    @Override
    public void run() {
        System.out.println("Thread started for client: " + clientSocket.getInetAddress().getHostAddress());

        try (
            // Automatically close resources when done
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true) // autoFlush true
        ) {
            String line;
            // The client may send multiple commands on a single connection
            while ((line = reader.readLine()) != null) {
                System.out.println("Received from client: " + line);
                
                String[] parts = line.split(" ", 3); // Split into Command, Filename, and Content (if needed)

                try {
                    // All file system operations are handled within a try-catch to 
                    // report specific errors without crashing the thread.
                    String command = parts[0].toUpperCase();
                    switch (command) {
                        case "CREATE":
                            handleCreate(parts, writer);
                            break;

                        case "WRITE":
                            handleWrite(parts, writer);
                            break;

                        case "READ":
                            handleRead(parts, writer);
                            break;

                        case "DELETE":
                            handleDelete(parts, writer);
                            break;

                        case "LIST":
                            handleList(writer);
                            break;

                        case "QUIT":
                            writer.println("Disconnecting.");
                            return; // Terminates the run() method and the thread

                        default:
                            writer.println("ERROR: Unknown command.");
                            break;
                    }
                } catch (Exception e) {
                    // Catch exceptions thrown by the fsManager methods
                    writer.println(e.getMessage());
                }
            }
        } catch (Exception e) {
            // Log the error but continue to allow server to run
            System.err.println("ClientHandler error for " + clientSocket.getInetAddress() + ": " + e.getMessage());
        } finally {
            try {
                // Ensure the client socket is always closed
                clientSocket.close();
                System.out.println("Client disconnected: " + clientSocket.getInetAddress().getHostAddress());
            } catch (Exception e) {
                // Ignore silent close errors
            }
        }
    }

    // --- Command Handlers ---

    private void handleCreate(String[] parts, PrintWriter writer) throws Exception {
        if (parts.length < 2) {
            writer.println("ERROR: CREATE command requires a filename.");
            return;
        }
        String filename = parts[1];

        // Assumes fsManager.createFile throws an exception on error
        fsManager.createFile(filename);
        writer.println("OK");
    }

    private void handleWrite(String[] parts, PrintWriter writer) throws Exception {
        if (parts.length < 3) {
            writer.println("ERROR: WRITE command requires a filename and content.");
            return;
        }
        String filename = parts[1];
        String content = parts[2];
        
        // Convert content string to bytes for the writeFile method
        byte[] contentBytes = content.getBytes(); 
        
        // Assumes fsManager.writeFile throws specific exceptions
        fsManager.writeFile(filename, contentBytes);
        writer.println("OK");
    }
    
    private void handleRead(String[] parts, PrintWriter writer) throws Exception {
        if (parts.length < 2) {
            writer.println("ERROR: READ command requires a filename.");
            return;
        }
        String filename = parts[1];
        
        // Assumes fsManager.readFile throws exception if file doesn't exist
        byte[] fileContents = fsManager.readFile(filename);
        
        // Send the file content back to the client (just the content, no prefix)
        String contentString = new String(fileContents);
        writer.println(contentString);
    }

    private void handleDelete(String[] parts, PrintWriter writer) throws Exception {
        if (parts.length < 2) {
            writer.println("ERROR: DELETE command requires a filename.");
            return;
        }
        String filename = parts[1];
        
        // Assumes fsManager.deleteFile throws exception if file doesn't exist
        fsManager.deleteFile(filename);
        writer.println("OK");
    }

    private void handleList(PrintWriter writer) {
        // Assumes fsManager.listFiles returns String[] of filenames
        String[] filenames = fsManager.listFiles();
        
        if (filenames.length == 0) {
            writer.println("");
        } else {
            // Send comma-separated filenames (no "SUCCESS:" prefix)
            writer.println(String.join(",", filenames));
        }
    }
}
