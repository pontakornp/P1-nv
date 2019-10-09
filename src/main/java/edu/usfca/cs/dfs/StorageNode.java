package edu.usfca.cs.dfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.util.CheckSum;
import edu.usfca.cs.dfs.util.CompressDecompress;
import edu.usfca.cs.dfs.util.Entropy;

public class StorageNode {
	private String storageNodeId;
	private String storageNodeAddr;
	private Integer storageNodePort;
	private Integer currentStorageNodeValue;
	private String fileStorageLocation;
	private ArrayList<String> replicationNodeIds;

	public StorageNode() {
		Config config = new Config();
		String storageDirectoryPath = config.getstorageDirectoryPath();
		registerNode(storageDirectoryPath);
	}

	/*
	 * This send a request to controller to register the node onto the controller
	 * @request parameters : None
	 * @return type: None
	 */
	public void registerNode(String storageDirectoryPath) {
		String nodeId = "1"; // get nodeId from controller
		this.storageNodeId = nodeId;
		this.storageNodeAddr = storageDirectoryPath + "/" + nodeId;
	}

	public String getStorageNodeId() {
		return this.storageNodeId;
	}

	public ArrayList<String> getReplicationNodeIds() {
		return this.replicationNodeIds;
	}
	
	public void setReplicationNodeIds(ArrayList<String> replicationNodesIdList) {
		this.replicationNodeIds = replicationNodesIdList;
	}

	public void registerNode() {
		
	}

	private boolean isFileCorrupted(String fileName, byte[] chunkData, String originalCheckSum) {
		// store file
		// call checksum method
		String currentCheckSum = CheckSum.checkSum(chunkData);
		// return true if the checksum matches else return false
		if (currentCheckSum.equals(originalCheckSum)) {
			return true;
		} else {
			return false;
		}
	}

	// To store chunk in a file,
	// 1. calculate Shannon Entropy of the files.
	// If their maximum compression is greater than 0.6 (1 - (entropy bits / 8)), then the chunk should be compressed.
	// 2. store the chunk
	// 3. do check sum if the it is corrupted or not
	// return true if the chunk is not corrupted, else return false
	public boolean storeChunk(String fileName, Integer chunkNumber, byte[] chunkData, String originalCheckSum) {
		// calculate Shannon Entropy
		double entropyBits = Entropy.calculateShannonEntropy(chunkData);
		double maximumCompression = 1 - (entropyBits / 8);
		byte[] data = chunkData;
		// if maximum compression is greater than 0.6, then compress the chunk data
		// else do not compress
		// then store the compress or uncompressed chunk data in a file
		if (maximumCompression > 0.6) {
			data = CompressDecompress.compress(chunkData);
			if (data == null) {
				return false;
			}
		}
		// store the chunk in a file
		try {
			OutputStream outputStream = new FileOutputStream(this.storageNodeAddr, true);
			outputStream.write(data);
		} catch (IOException e) {
			System.out.println("There is a problem when writing stream to file.");
			return false;
		}
		// check sum if the file is corrupted or not
		return isFileCorrupted(fileName, chunkData, originalCheckSum);
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

	// send heartbeat to controller to inform that storage node is still available
	public void sendHeartBeat() {
		
	}
}
