package edu.usfca.cs.dfs;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.util.CheckSum;
import edu.usfca.cs.dfs.util.CompressDecompress;
import edu.usfca.cs.dfs.util.Entropy;

import javax.xml.bind.DatatypeConverter;

public class StorageNode {
	private String storageNodeId;
	private String strorageNodeAddr;
	private Integer storageNodePort;
	private Integer currentStorageNodeValue;
	private String fileStorageLocation;
	private ArrayList<String> replicationStorageNodeIds;
	
	public String getStorageNodeId() {
		return this.storageNodeId;
	}

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
		this.strorageNodeAddr = storageDirectoryPath + "/" + nodeId;
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
	public boolean storeChunk(String fileName, Integer chunkNumber, byte[] chunkData) {
		// calculate Shannon Entropy
		double entropy = Entropy.calculateShannonEntropy(chunkData);
		int entropyBits = chunkData.length;
		double compressionBaseline = 0.6 * (1 - (entropyBits / 8));
		byte[] data = chunkData;
		if (entropy > compressionBaseline) {
			//compress
			data = CompressDecompress.compress(chunkData);
			if (data == null) {
				return false;
			}
		}
		// store the chunk in a file
		try {
			OutputStream outputStream = new FileOutputStream(this.strorageNodeAddr, true);
			outputStream.write(data);
		} catch (IOException e) {
			System.out.println("There is a problem when writing stream to file.");
			return false;
		}
		// check sum if the file is corrupted or not

		return true;
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
