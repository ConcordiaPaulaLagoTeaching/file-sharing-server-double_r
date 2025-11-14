package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;

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
                String command = parts[0].toUpperCase();

                try {
                    // All file system operations are handled within a try-catch to 
                    // report specific errors without crashing the thread.
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
                            writer.println("SUCCESS: Disconnecting.");
                            return; // Terminates the run() method and the thread

                        default:
                            writer.println("ERROR: Unknown command.");
                            break;
                    }
                } catch (Exception e) {
                    // Catch exceptions thrown by the fsManager methods
                    writer.println("ERROR: " + e.getMessage());
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
        
        if (filename.length() > 11) {
            writer.println("ERROR: filename too large");
            return;
        }

        // Assumes fsManager.createFile throws an exception on resource failure
        fsManager.createFile(filename);
        writer.println("SUCCESS: File '" + filename + "' created.");
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
        
        // Assumes fsManager.writeFile throws specific exceptions:
        // - "file <filename> does not exist"
        // - "file too large" (if not enough free blocks)
        fsManager.writeFile(filename, contentBytes);
        writer.println("SUCCESS: Data written to file '" + filename + "'.");
    }
    
    private void handleRead(String[] parts, PrintWriter writer) throws Exception {
        if (parts.length < 2) {
            writer.println("ERROR: READ command requires a filename.");
            return;
        }
        String filename = parts[1];
        
        // Assumes fsManager.readFile throws "file <filename> does not exist"
        byte[] fileContents = fsManager.readFile(filename);
        
        // Send the file content back to the client
        String contentString = new String(fileContents);
        writer.println("SUCCESS: " + contentString);
    }

    private void handleDelete(String[] parts, PrintWriter writer) throws Exception {
        if (parts.length < 2) {
            writer.println("ERROR: DELETE command requires a filename.");
            return;
        }
        String filename = parts[1];
        
        // Assumes fsManager.deleteFile throws "file <filename> does not exist"
        fsManager.deleteFile(filename);
        writer.println("SUCCESS: File '" + filename + "' deleted.");
    }

    private void handleList(PrintWriter writer) {
        // Assumes fsManager.listFiles returns String[] of filenames
        String[] filenames = fsManager.listFiles();
        
        if (filenames.length == 0) {
            writer.println("SUCCESS: No files in the file system.");
        } else {
            // Send a single response line containing all filenames separated by a delimiter
            writer.println("SUCCESS: " + String.join(", ", filenames));
        }
    }
}