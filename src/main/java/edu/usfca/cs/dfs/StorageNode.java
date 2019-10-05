package edu.usfca.cs.dfs;

import java.util.concurrent.ConcurrentHashMap;

public class StorageNode {
	public String nodeId;
	private String nodeAddress;
	private BloomFilter bloomFilter;
	private ConcurrentHashMap<String, Chunk> chunckMetaData; 
	
	public synchronized Integer getChunckCount(String fileName) {
		return 0;
	}
	
	public synchronized String getChunckLocation(String fileName, Integer chunckNumber) {
		return "";
	}
	
	public synchronized byte[] retreiveChunck() {
		return null;
	}

	// To store chunk in a file,
	// 1. check Shannon Entropy of the files.
	// If their maximum compression is greater than 0.6 (1 - (entropy bits / 8)), then the chunk should be compressed.
	// 2. store the chunk in a file
	// 3. do check sum if the it is corrupted or not
	// return true if the chunk is not corrupted, else return false
	public boolean storeChunk(String fileName, Integer chunkNumber, byte[] chunkData) {


		return true;
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
	public synchronized ConcurrentHashMap<String, Chunk> getChunkMetaData() {
		return this.chunkMetaData;
	}

	// send heartbeat to controller to inform that storage node is still available
	public void sendHeartBeat() {
		
	}
}
