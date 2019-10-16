package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

public class ChunkMetaData {
    public String fileName;
    public long fileSize;
    public int chunkId;
    public int chunkSize;
    public int maxChunkId;
    public String checkSum;
    public boolean isCompressed;
    public boolean isPrimary;

    public ChunkMetaData() {

    }

    public ChunkMetaData(String fileName, long fileSize, int chunkId, int chunkSize, int maxChunkId, String checkSum, boolean isCompressed) {
    	this.fileName = fileName;
    	this.fileSize = fileSize;
    	this.chunkId = chunkId;
    	this.chunkSize = chunkSize;
    	this.maxChunkId = maxChunkId;
    	this.checkSum = checkSum;
        this.isCompressed = isCompressed;
    }

    public void setChunkMetaDataWithFilePath(String filePath) {
        this.setVariables(filePath);
    }

    public boolean setVariables(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            System.out.println("Meta file not found at given path : "+ filePath);
        }
        System.out.println("Meta file found at given path : "+ filePath);

        try {
            JsonReader jsonReader = new JsonReader(new FileReader(filePath));
            Gson gson = new Gson();
            ChunkMetaData chunkMetaData = gson.fromJson(jsonReader, ChunkMetaData.class);
            System.out.println(chunkMetaData.toString());
            this.fileName = chunkMetaData.fileName;
            this.chunkId = chunkMetaData.chunkId;
            this.chunkSize = chunkMetaData.chunkSize;
            this.maxChunkId = chunkMetaData.maxChunkId;
            this.checkSum = chunkMetaData.checkSum;
            this.isCompressed = chunkMetaData.isCompressed;
        } catch(IOException ioe) {
            ioe.printStackTrace();
            System.out.println("Please try again with correct meta data file.");
            return false;
        }
        return true;
    }

    public String getFileName() {
    	return this.fileName;
    }
    
    public void setFileName(String fileName) {
    	this.fileName = fileName;
    }
    
    public long getFileSize() {
    	return this.fileSize;
    }

    public void setFileSize(long fileSize) {
    	this.fileSize = fileSize;
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
