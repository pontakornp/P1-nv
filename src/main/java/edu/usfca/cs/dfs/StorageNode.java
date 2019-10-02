package edu.usfca.cs.dfs;

import java.util.ArrayList;

public class StorageNode {
	private ArrayList<String> activeStorageNodes;

	// store chunk
	public void storeChunk(String fileName, int chunkNum, byte[] chunkData) {

    }

    // get number of chunks
    public int getChunkNum(String fileName) {
        return -1;
    }

    // get chunk location
    public int getChunkLocation(String fileName, int chunkNum) {
	    return -1;
    }

    // retrieve chunk
    public byte[] retrieveChunk(String fileName, int chunkNum) {
	    return new byte[0];
    }

    // list chunks and file names
    public void listChunksAndFileNames() {

    }

    // send heartbeat to controller
    // default sending interval: 5 seconds
}
