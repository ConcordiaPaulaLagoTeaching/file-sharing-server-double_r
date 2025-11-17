package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private static FileSystemManager instance;
    private RandomAccessFile disk;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private static final int BLOCK_SIZE = 128;
    private static final int FENTRY_SIZE = 15; // 11 bytes name + 2 bytes size + 2 bytes firstblock
    private static final int FNODE_SIZE = 4;   // 2 bytes blockIndex + 2 bytes nextBlock

    private final FEntry[] inodeTable;
    private final FNode[] nodeTable;
    private final boolean[] freeBlockList;

    private final long METADATA_SIZE;
    private final long DATA_START_OFFSET;

    // -------------------- Constructor --------------------
    public FileSystemManager(String filename, int totalSize) {
        try {
            File f = new File(filename);
            
            // FIX 1: "rws" mode forces synchronous writes for persistence
            this.disk = new RandomAccessFile(f, "rws");

            this.inodeTable = new FEntry[MAXFILES];
            this.nodeTable = new FNode[MAXBLOCKS];
            this.freeBlockList = new boolean[MAXBLOCKS];

            this.METADATA_SIZE = (FENTRY_SIZE * MAXFILES) + (FNODE_SIZE * MAXBLOCKS);
            this.DATA_START_OFFSET = ((METADATA_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;

            if (!f.exists() || f.length() == 0) {
                initializeEmptyFileSystem();
            } else {
                loadFileSystem(); // This is where the bug was
            }

            instance = this;
            System.out.println("File system initialized. Data starts at byte " + DATA_START_OFFSET);

            // FIX 2: Shutdown hook for persistence
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown hook running: Closing RandomAccessFile...");
                this.close();
            }));
        } catch (IOException e) {
            throw new RuntimeException("Error initializing FileSystemManager: " + e.getMessage());
        }
    }

    // FIX 2 (Helper): Close method for shutdown hook
    public void close() {
        try {
            if (this.disk != null) {
                this.disk.close();
                System.out.println("RandomAccessFile closed successfully.");
            }
        } catch (IOException e) {
            System.err.println("Error closing RandomAccessFile: " + e.getMessage());
        }
    }

    // -------------------- Initialization --------------------
    private void initializeEmptyFileSystem() throws IOException {
        disk.seek(0);
        for (int i = 0; i < MAXFILES; i++) {
            writeEmptyFEntry(i);
        }
        for (int i = 0; i < MAXBLOCKS; i++) {
            writeFNode(i, -1, -1);
            nodeTable[i] = new FNode(-1);
            freeBlockList[i] = true;
        }
        disk.setLength(DATA_START_OFFSET + (MAXBLOCKS * BLOCK_SIZE));
        disk.getFD().sync();
    }

    private void loadFileSystem() throws IOException {
        disk.seek(0);
        byte[] nameBuffer = new byte[11];

        // Load FEntries
        for (int i = 0; i < MAXFILES; i++) {
            disk.readFully(nameBuffer);

            // Create a string from all 11 bytes
            String nameWithNuls = new String(nameBuffer, StandardCharsets.UTF_8);
            
            // Find the first NUL character
            int nulIndex = nameWithNuls.indexOf('\u0000');
            
            // If we found a NUL, cut the string off before it.
            // Otherwise, just use the string.
            String name = (nulIndex == -1) ? nameWithNuls.trim() : nameWithNuls.substring(0, nulIndex).trim();
            
            short size = disk.readShort();
            short firstBlock = disk.readShort();

            if (!name.isEmpty()) {
                inodeTable[i] = new FEntry(name, size, firstBlock);
            }
        }

        // Load FNodes (this part is fine)
        for (int i = 0; i < MAXBLOCKS; i++) {
            short blockIndex = disk.readShort();
            short next = disk.readShort();
            nodeTable[i] = new FNode(blockIndex);
            nodeTable[i].setNext(next);
            freeBlockList[i] = (blockIndex < 0);
        }
    }
    // ----------------------------------------------------------


    // -------------------- Create --------------------
    public void createFile(String fileName) throws Exception {
        lock.writeLock().lock();
        try {
            // FIX 3: "filename too long" for test
            if (fileName.length() > 11) {
                throw new Exception("ERROR: filename too long");
            }

            // FIX 4: Silent return if file exists for test
            for (FEntry entry : inodeTable) {
                if (entry != null && entry.getFilename().equals(fileName)) {
                    return;
                }
            }

            int freeIndex = -1;
            for (int i = 0; i < MAXFILES; i++) {
                if (inodeTable[i] == null) {
                    freeIndex = i;
                    break;
                }
            }
            if (freeIndex == -1) {
                throw new Exception("ERROR: Maximum file limit reached");
            }

            inodeTable[freeIndex] = new FEntry(fileName, (short) 0, (short) -1);
            writeFEntry(freeIndex, inodeTable[freeIndex]);
            disk.getFD().sync(); // Force sync for persistence
        } finally {
            lock.writeLock().unlock();
        }
    }

    // -------------------- Write --------------------
    public void writeFile(String fileName, byte[] contents) throws Exception {
        lock.writeLock().lock();
        try {
            FEntry file = findFile(fileName);
            if (file == null) throw new Exception("ERROR: file " + fileName + " does not exist");

            int neededBlocks = (int) Math.ceil((double) contents.length / BLOCK_SIZE);
            int[] freeBlocks = findFreeBlocks(neededBlocks);
            if (freeBlocks == null) throw new Exception("ERROR: file too large or insufficient space");

            clearFileBlocks(file);

            short firstBlock = (short) freeBlocks[0];
            file.setFilesize((short) contents.length);
            file.setFirstBlock(firstBlock);

            for (int i = 0; i < neededBlocks; i++) {
                int blockIndex = freeBlocks[i];
                int nextBlock = (i < neededBlocks - 1) ? freeBlocks[i + 1] : -1;

                freeBlockList[blockIndex] = false;
                nodeTable[blockIndex] = new FNode(blockIndex);
                nodeTable[blockIndex].setNext(nextBlock);
                writeFNode(blockIndex, blockIndex, nextBlock);

                long dataPos = DATA_START_OFFSET + (blockIndex * BLOCK_SIZE);
                disk.seek(dataPos);
                int length = Math.min(BLOCK_SIZE, contents.length - (i * BLOCK_SIZE));
                disk.write(contents, i * BLOCK_SIZE, length);
            }

            writeFEntry(findFEntryIndex(fileName), file);
            disk.getFD().sync(); // Force sync for persistence
        } finally {
            lock.writeLock().unlock();
        }
    }

    // -------------------- Read --------------------
    public byte[] readFile(String fileName) throws Exception {
        lock.readLock().lock();
        try {
            FEntry file = findFile(fileName);
            if (file == null) throw new Exception("ERROR: file " + fileName + " does not exist");

            byte[] data = new byte[file.getFilesize()];
            int bytesRead = 0;
            short currentNodeIndex = file.getFirstBlock();

            while (currentNodeIndex != -1 && bytesRead < file.getFilesize()) {
                FNode node = nodeTable[currentNodeIndex];
                long dataPos = DATA_START_OFFSET + (node.getBlockIndex() * BLOCK_SIZE);
                disk.seek(dataPos);

                int bytesToRead = Math.min(BLOCK_SIZE, file.getFilesize() - bytesRead);
                disk.readFully(data, bytesRead, bytesToRead);
                bytesRead += bytesToRead;
                currentNodeIndex = (short) node.getNext();
            }
            return data;
        } finally {
            lock.readLock().unlock();
        }
    }

    // -------------------- Delete --------------------
    public void deleteFile(String fileName) throws Exception {
        lock.writeLock().lock();
        try {
            int index = findFEntryIndex(fileName);
            if (index == -1) throw new Exception("ERROR: file " + fileName + " does not exist");

            FEntry entry = inodeTable[index];
            clearFileBlocks(entry);

            inodeTable[index] = null;
            writeEmptyFEntry(index);
            disk.getFD().sync(); // Force sync for persistence
        } finally {
            lock.writeLock().unlock();
        }
    }

    // -------------------- List --------------------
    public String[] listFiles() {
        lock.readLock().lock();
        try {
            ArrayList<String> files = new ArrayList<>();
            for (FEntry entry : inodeTable) {
                if (entry != null) files.add(entry.getFilename());
            }
            return files.toArray(new String[0]);
        } finally {
            lock.readLock().unlock();
        }
    }

    // -------------------- Utility Methods --------------------
    private FEntry findFile(String name) {
        for (FEntry e : inodeTable) {
            if (e != null && e.getFilename().equals(name)) return e;
        }
        return null;
    }

    private int findFEntryIndex(String name) {
        for (int i = 0; i < MAXFILES; i++) {
            if (inodeTable[i] != null && inodeTable[i].getFilename().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private int[] findFreeBlocks(int count) {
        ArrayList<Integer> free = new ArrayList<>();
        for (int i = 0; i < MAXBLOCKS; i++) {
            if (freeBlockList[i]) free.add(i);
            if (free.size() == count) break;
        }
        return (free.size() == count) ? free.stream().mapToInt(Integer::intValue).toArray() : null;
    }

    private void clearFileBlocks(FEntry file) throws IOException {
        short nodeIndex = file.getFirstBlock();
        while (nodeIndex != -1) {
            FNode node = nodeTable[nodeIndex];
            long dataPos = DATA_START_OFFSET + (node.getBlockIndex() * BLOCK_SIZE);
            disk.seek(dataPos);
            disk.write(new byte[BLOCK_SIZE]); // overwrite with zeroes
            freeBlockList[nodeIndex] = true;
            nodeIndex = (short) node.getNext();
            writeFNode(node.getBlockIndex(), -1, -1);
        }
        disk.getFD().sync();
    }

    private void writeFEntry(int index, FEntry entry) throws IOException {
        long pos = index * FENTRY_SIZE;
        disk.seek(pos);

        byte[] nameBytes = new byte[11];
        byte[] fn = entry.getFilename().getBytes(StandardCharsets.UTF_8);
        System.arraycopy(fn, 0, nameBytes, 0, Math.min(fn.length, 11));
        disk.write(nameBytes);
        disk.writeShort(entry.getFilesize());
        disk.writeShort(entry.getFirstBlock());
    }

    private void writeEmptyFEntry(int index) throws IOException {
        long pos = index * FENTRY_SIZE;
        disk.seek(pos);
        disk.write(new byte[FENTRY_SIZE]);
    }

    private void writeFNode(int index, int blockIndex, int next) throws IOException {
        long pos = (MAXFILES * FENTRY_SIZE) + (index * FNODE_SIZE);
        disk.seek(pos);
        disk.writeShort(blockIndex);
        disk.writeShort(next);
    }
}