package edu.usfca.cs.dfs;

import java.util.concurrent.ConcurrentHashMap;

public class StorageNode {
	public String nodeId;
	private String nodeAddress;
	private BloomFilter bloomFilter;
	private ConcurrentHashMap<String, Chunck> chunkMetaData;

	// store chunk in a file
	public void storeChunk(String fileName, Integer chunkNumber, byte[] chunkData) {
		
	}

	// get number of chunks
	public Integer getChunkCount(String fileName) {
		return 0;
	}

	// get chunk location
	public String getChunkLocation(String fileName, Integer chunkMetaData) {
		return "";
	}

	// retrieve chunk from a file
	public byte[] retrieveChunk(String fileName, Integer chunkNumber) {
		return null;
	}

	// list chunks and file names
	public void listChunksAndFileNames() {

	}

	// get chunk meta data
	public synchronized ConcurrentHashMap<String, Chunck> getChunkMetaData() {
		return this.chunkMetaData;
	}

	// send heartbeat to controller to inform that storage node is still available
	public void sendHeartBeat() {
		
	}
}
