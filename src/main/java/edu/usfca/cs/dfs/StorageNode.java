package edu.usfca.cs.dfs;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class StorageNode {
	public String nodeId;
	private String nodeAddress;
	private BloomFilter bloomFilter;
	private ConcurrentHashMap<String, Chunk> chunkMetaData;

	private double Log2(double n) {
		return Math.log(n) / Math.log(2);
	}

	private double calculateShannonEntropy(byte[] chunkData) {
		HashMap counter = new HashMap();
		int totalCount = 0;
		for(int i = 0; i < chunkData.length; i++) {
			int bit = chunkData[i];
			if (counter.get(bit) == null) {
				counter.put(bit, 1);
			} else {
				counter.put(bit, (Integer)counter.get(bit) + 1);
			}
			totalCount++;
		}
		double entropy = 0;
		for(int i = 0; i < chunkData.length; i++){
			int bit = chunkData[i];
			int freq = (Integer)counter.get(bit);
			double prob = (double) freq / totalCount;
			entropy -= prob * Log2(prob);
		}
		return entropy;
	}

	// To store chunk in a file,
	// 1. check Shannon Entropy of the files.
	// If their maximum compression is greater than 0.6 (1 - (entropy bits / 8)), then the chunk should be compressed.
	// 2. store the chunk
	// 3. do check sum if the it is corrupted or not
	// return true if the chunk is not corrupted, else return false
	public boolean storeChunk(String fileName, Integer chunkNumber, byte[] chunkData) {
		double entropy = calculateShannonEntropy(chunkData);
		int entropyBits = chunkData.length;
		double compressionBaseline = 0.6 * (1 - (entropyBits / 8));
		if (entropy > compressionBaseline) {
			//compress
		}
		return false;
	}


	// get number of chunks
	public synchronized Integer getChunkCount(String fileName) {
		return 0;
	}
	// get chunk location
	public synchronized String getChunkLocation(String fileName, Integer chunkNumber) {
		return "";
	}

	// retrieve chunk from a file
	public synchronized byte[] retrieveChunk(String fileName, Integer chunkNumber) {
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
