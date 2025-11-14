package ca.concordia.filesystem.datastructures;

public class FNode {

    private short blockIndex;
    private short next;

    public FNode(short blockIndex) {
        this.blockIndex = blockIndex;
        this.next = -1;
    }

    // Added minimal getters and setters used by FileSystemManager
    public short getBlockIndex() {
        return blockIndex;
    }

    public void setBlockIndex(short blockIndex) {
        this.blockIndex = blockIndex;
    }

    public short getNext() {
        return next;
    }

    public void setNext(short next) {
        this.next = next;
    }
}

