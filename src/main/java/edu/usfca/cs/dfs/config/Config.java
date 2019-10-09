package edu.usfca.cs.dfs.config;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.FileReader;
import java.io.IOException;

/**
 * 
 * @author pontakornp
 * 
 * 
 * Manages config file of non-blocking file transfer
 */
public class Config {
	private String clientAddr;
	private String storageNodeAddr;
	private String controllerAddr;
	private int chunkSize;
	private int storageNodePort;

	/**
	 * Set variables from the config.json file and assign them to variables in this class.
	 */
	public boolean setVariables() {
		Config config = new Config();
		try {
			JsonReader jsonReader = new JsonReader(new FileReader("config.json"));
			Gson gson = new Gson();
			config = gson.fromJson(jsonReader, Config.class);
			this.clientAddr = config.clientAddr;
			this.storageNodeAddr = config.storageNodeAddr;
			this.controllerAddr = config.controllerAddr;
			this.chunkSize = config.chunkSize;
			this.storageNodePort = config.storageNodePort;
		} catch(IOException ioe) {
			System.out.println("Please try again with correct config file.");
			return false;
		}
		return true;
	}

	public String getClientAddr() {
		return clientAddr;
	}

	public void setClientAddr(String clientAddr) {
		this.clientAddr = clientAddr;
	}

	public String getStorageNodeAddr() {
		return storageNodeAddr;
	}

	public void setStorageNodeAddr(String storageNodeAddr) {
		this.storageNodeAddr = storageNodeAddr;
	}

	public String getControllerAddr() {
		return controllerAddr;
	}

	public void setControllerAddr(String controllerAddr) {
		this.controllerAddr = controllerAddr;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public int getStorageNodePort() {
		return storageNodePort;
	}

	public void setStorageNodePort(int storageNodePort) {
		this.storageNodePort = storageNodePort;
	}
}