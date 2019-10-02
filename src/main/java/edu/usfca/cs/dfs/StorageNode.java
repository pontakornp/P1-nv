package edu.usfca.cs.dfs;

import java.util.concurrent.ConcurrentHashMap;

public class StorageNode {
	public String nodeId;
	private String nodeAddress;
	private BloomFilter bloomFilter;
	private ConcurrentHashMap<String, Chunck> chunckMetaData; 
	
	public void storeChunk(String fileName, Integer chunkNumber, byte[] chunkData) {
		
	}
	
	public Integer getChunckCount(String fileName) {
		return 0;
	}
	
	public String getChunckLocation(String fileName, Integer chunckNumber) {
		return "";
	}
	
	public byte[] retreiveChunck() {
		return null;
	}
	
	public synchronized ConcurrentHashMap<String, Chunck> getChunckMetaData() {
		return this.chunckMetaData;
	}
	
	public void sendHeartBeat() {
		
	}
}
