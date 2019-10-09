package edu.usfca.cs.dfs;

public class Chunk {
    private String checkSum;
    private boolean isCompressed;
    private int chunkNum;
    private int chunkSize;

    public Chunk(String checkSum, boolean isCompressed, int chunkNum, int chunkSize) {
        this.checkSum = checkSum;
        this.isCompressed = isCompressed;
        this.chunkNum = chunkNum;
        this.chunkSize = chunkSize;
    }

    public String getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(String checkSum) {
        this.checkSum = checkSum;
    }

    public boolean isCompressed() {
        return isCompressed;
    }

    public void setCompressed(boolean compressed) {
        isCompressed = compressed;
    }

    public int getChunkNum() {
        return chunkNum;
    }

    public void setChunkNum(int chunkNum) {
        this.chunkNum = chunkNum;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
}
