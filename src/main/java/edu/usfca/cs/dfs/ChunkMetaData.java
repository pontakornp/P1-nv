package edu.usfca.cs.dfs;

public class ChunkMetaData {
    private String fileName;
    private int chunkNum;
    private int maxChunkNum;
    private String checkSum;
    private boolean isCompressed;
    private boolean isPrimary;

    public ChunkMetaData(String checkSum, boolean isCompressed, int chunkNum, int chunkSize) {
        this.checkSum = checkSum;
        this.isCompressed = isCompressed;
        this.chunkNum = chunkNum;
    }
    public String getFileName() {
    	return this.fileName;
    }
    
    public void setFileName(String fileName) {
    	this.fileName = fileName;
    }
    
    public int getChunkNum() {
        return chunkNum;
    }

    public void setChunkNum(int chunkNum) {
        this.chunkNum = chunkNum;
    }
    
    public int getMaxChunkNum() {
        return maxChunkNum;
    }

    public void setMaxChunkNum(int maxChunkNum) {
        this.maxChunkNum = maxChunkNum;
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

    public boolean isPrimary() {
        return this.isPrimary;
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }
}
