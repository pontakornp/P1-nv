package edu.usfca.cs.dfs;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import edu.usfca.cs.dfs.config.Config;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class StorageNode {
	public String nodeId;
	private String nodeAddress;
	private BloomFilter bloomFilter;
	private ConcurrentHashMap<String, Chunk> chunkMetaData;

	public StorageNode() {
		Config config = new Config();
		String storageDirectoryPath = config.getstorageDirectoryPath();
		registerNode(storageDirectoryPath);
	}

	// register node with controller
	public void registerNode(String storageDirectoryPath) {
		String nodeId = "1"; // get nodeId from controller
		this.nodeId = nodeId;
		this.nodeAddress = storageDirectoryPath + "/" + nodeId;
	}

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

	private boolean checkSum(String fileName, byte[] chunkData) throws NoSuchAlgorithmException, IOException {
		String checksum = "5EB63BBBE01EEED093CB22BB8F5ACDC3";
		byte[] digest = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(Files.readAllBytes(Paths.get(fileName)));
			digest = md.digest();
		} catch (NoSuchAlgorithmException | IOException e) {
			System.out.println("Algorithm or file does not exist.");
			return false;
		}
		String myChecksum = DatatypeConverter.printHexBinary(digest).toUpperCase();
		if (myChecksum.equals(checksum)) {
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
		double entropy = calculateShannonEntropy(chunkData);
		int entropyBits = chunkData.length;
		double compressionBaseline = 0.6 * (1 - (entropyBits / 8));
		byte[] data = chunkData;
		if (entropy > compressionBaseline) {
			//compress
			data = compress(chunkData);
			if (data == null) {
				return false;
			}
		}
		// store the chunk in a file
		try {
			OutputStream outputStream = new FileOutputStream(nodeAddress, true);
			outputStream.write(data);
		} catch (IOException e) {
			System.out.println("There is a problem when writing stream to file.");
			return false;
		}
		// check sum if the file is corrupted or not

		return true;
	}

	// Reference: https://dzone.com/articles/how-compress-and-uncompress
	public byte[] compress(byte[] data) {
		byte[] output = null;
		try {
			Deflater deflater = new Deflater();
			deflater.setInput(data);
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
			deflater.finish();
			byte[] buffer = new byte[1024];
			while (!deflater.finished()) {
				int count = deflater.deflate(buffer);
				outputStream.write(buffer, 0, count);
			}
			outputStream.close();
			output = outputStream.toByteArray();
		} catch (IOException e) {
			System.out.println("Fail to compress chunk.");
		}
		return output;

	}

	// Reference: https://dzone.com/articles/how-compress-and-uncompress
	public byte[] decompress(byte[] data) {
		byte[] output = null;
		try {
			Inflater inflater = new Inflater();
			inflater.setInput(data);
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
			byte[] buffer = new byte[1024];
			while (!inflater.finished()) {
				int count = inflater.inflate(buffer);
				outputStream.write(buffer, 0, count);
			}
			outputStream.close();
			output = outputStream.toByteArray();
		} catch (IOException | DataFormatException e) {
			System.out.println("Fail to decompress chunk.");
		}
		return output;
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
