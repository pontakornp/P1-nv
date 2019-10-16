package edu.usfca.cs.dfs;

public class ChunkMetaData {
    public String fileName;
    public int chunkId;
    public int chunkSize;
    public int maxChunkId;
    public String checkSum;
    public boolean isCompressed;
    public boolean isPrimary;

    public ChunkMetaData(String fileName, int chunkId, int chunkSize, int maxChunkId, String checkSum, boolean isCompressed) {
    	this.fileName = fileName;
    	this.chunkId = chunkId;
    	this.chunkSize = chunkSize;
    	this.maxChunkId = maxChunkId;
    	this.checkSum = checkSum;
        this.isCompressed = isCompressed;
    }
    public String getFileName() {
    	return this.fileName;
    }
    
    public void setFileName(String fileName) {
    	this.fileName = fileName;
    }
    
    public int getchunkId() {
        return chunkId;
    }

    public void setchunkId(int chunkId) {
        this.chunkId = chunkId;
    }
    
    public int getMaxchunkId() {
        return maxChunkId;
    }

    public void setMaxchunkId(int maxChunkId) {
        this.maxChunkId = maxChunkId;
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
