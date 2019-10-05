package edu.usfca.cs.dfs;

import java.util.concurrent.ConcurrentHashMap;

public class StorageNode {
	public String nodeId;
	private String nodeAddress;
	private BloomFilter bloomFilter;
	private ConcurrentHashMap<String, Chunck> chunckMetaData; 
	
	public synchronized void storeChunk(String fileName, Integer chunkNumber, byte[] chunkData) {
		
	}
	
	public synchronized Integer getChunckCount(String fileName) {
		return 0;
	}
	
	public synchronized String getChunckLocation(String fileName, Integer chunckNumber) {
		return "";
	}
	
	public synchronized byte[] retreiveChunck() {
		return null;
	}
	
	public synchronized ConcurrentHashMap<String, Chunck> getChunckMetaData() {
		return this.chunckMetaData;
	}
	
	public synchronized void sendHeartBeat() {
		
	}
}
