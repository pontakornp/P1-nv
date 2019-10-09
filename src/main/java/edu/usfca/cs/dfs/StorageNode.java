package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.util.CheckSum;
import edu.usfca.cs.dfs.util.CompressDecompress;
import edu.usfca.cs.dfs.util.Entropy;

public class StorageNode {
	private String storageNodeId;
	private String storageNodeAddr;
	private String storageNodeFileDirectory; // storageNodeAddr + storageNodeAddr + '/'
	private Integer storageNodePort;
	private Integer currentStorageNodeValue;
	private String fileStorageLocation;
	private ArrayList<String> replicationNodeIds;
	private HashMap<String, Chunk> metaDataMap;

	public StorageNode() {
		Config config = new Config();
		config.setVariables();
		registerNode(config.getStorageNodeAddr());
		metaDataMap = new HashMap<String, Chunk>();
	}

	/*
	 * This send a request to controller to register the node onto the controller
	 * @request parameters : None
	 * @return type: None
	 */
	public void registerNode(String storageNodeAddr) {
		String nodeId = "1"; // get nodeId from controller
		this.storageNodeId = nodeId;
		this.storageNodeAddr = storageNodeAddr;
		this.storageNodeFileDirectory = storageNodeAddr + nodeId + '/';
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

	private boolean isFileCorrupted(byte[] chunkData, String originalCheckSum) {
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
	// 1. calculate Shannon Entropy of the files which is the maximum compression
	// If their maximum compression is greater than (1 - (entropy bits / 8)), then the chunk should be compressed.
	// 2. store the chunk
	// 3. do check sum if the it is corrupted or not
	// return true if the chunk is not corrupted, else return false
	public boolean storeChunk(String fileName, int chunkNumber, byte[] chunkData, String originalCheckSum) {
		// calculate Shannon Entropy
		double entropyBits = Entropy.calculateShannonEntropy(chunkData);
		double maximumCompression = 1 - (entropyBits / 8);
		byte[] data = chunkData;
		// if maximum compression is greater than 0.6, then compress the chunk data
		// else do not compress
		// then store the compress or uncompressed chunk data in a file
		boolean isCompressed = false;
		if (maximumCompression > 0.6) {
			data = CompressDecompress.compress(chunkData);
			if (data == null) {
				return false;
			}
			isCompressed = true;
		}
		// create directory
		File dir = new File(this.storageNodeFileDirectory);
		if (!dir.exists()) {
			dir.mkdir();
		}
		// store the chunk in a file
		try {
			OutputStream outputStream = new FileOutputStream(this.storageNodeFileDirectory + fileName);
			outputStream.write(data);
			byte[] storedChunkData = Files.readAllBytes(Paths.get(this.storageNodeFileDirectory + fileName));
			// check sum if the file is corrupted or not
			if (!isFileCorrupted(storedChunkData, originalCheckSum)) {
				// add chunk to the meta data map
				Chunk chunkObj = new Chunk(originalCheckSum, isCompressed, chunkNumber, chunkData.length);
				String chunkKey = fileName + '_' + chunkNumber;
				metaDataMap.put(chunkKey, chunkObj);
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			System.out.println("There is a problem when writing stream to file.");
			return false;
		}
	}

	public static void main(String args[]) {
		String checkSum = "098f6bcd4621d373cade4e832627b4f6";
		try {
			byte[] temp = Files.readAllBytes(Paths.get("/Users/pontakornp/Documents/projects/bigdata/P1-nv/test.jpg"));
			StorageNode sn = new StorageNode();
			System.out.println(sn.storeChunk("test2.jpg", 1, temp, checkSum));
		} catch (IOException e) {
			e.printStackTrace();
		}
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
		// 1. form a file name
		String filePath = storageNodeFileDirectory + fileName;
		// 2. check if file exist in the meta data map
		if (!metaDataMap.containsKey(filePath)) {
			return null;
		}
		Chunk chunk = metaDataMap.get(filePath);
		// 3. if chunk is compressed, need to decompress the byte array data before returning it
		// else return the byte array data right away
		try {
			byte[] chunkData = Files.readAllBytes(Paths.get(filePath));
			if (chunk.isCompressed()) {
				chunkData = CompressDecompress.decompress(chunkData);
			}
			return chunkData;
		} catch (IOException e) {
			System.out.println("Fail to retrieve chunk.");
			return null;
		}
	}

	// list chunks and file names
	public void listChunksAndFileNames() {

	}

	// send heartbeat to controller to inform that storage node is still available
	public void sendHeartBeat() {
		
	}
}
