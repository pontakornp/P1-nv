package edu.usfca.cs.dfs.config;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import edu.usfca.cs.dfs.StorageNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author pontakornp
 * Manages config file of non-blocking file transfer
 */
public class Config {
	
	static Logger logger = LogManager.getLogger(Config.class);
	
	private String clientAddr;
	private String clientDirectoryPath;
	private String storageNodeAddr;
	private int storageNodePort;
	private String storageDirectoryPath;
	private int maxStorageValue;
	
	private String controllerAddr;
	private int chunkSize;
	
	public Config(String fileName) {
		this.setVariables(fileName);
	}
	

	/**
	 * Set variables from the config.json file and assign them to variables in this class.
	 */
	public boolean setVariables(String fileName) {
		File file = new File(fileName);
        if (!file.exists()) {
        	System.out.println("Config file not found at given path : "+ fileName);
        }
    	System.out.println("Config file found at given path : "+ fileName);
		
		try {
			JsonReader jsonReader = new JsonReader(new FileReader(fileName));
			Gson gson = new Gson();
			Config config = gson.fromJson(jsonReader, Config.class);
			System.out.println(config);
			this.clientAddr = config.clientAddr;
			this.controllerAddr = config.controllerAddr;
			
			
			this.storageNodeAddr = config.storageNodeAddr;
			this.storageNodePort = config.storageNodePort;
			this.storageDirectoryPath = config.storageDirectoryPath;
			this.chunkSize = config.chunkSize;
			this.maxStorageValue = config.maxStorageValue;
		} catch(IOException ioe) {
			System.out.println("Please try again with correct config file.");
			return false;
		}
		return true;
	}

	public String getClientDirectoryPath() {
		return this.clientDirectoryPath;
	}
	
	public String getClientAddr() {
		return this.clientAddr;
	}

	public void setClientAddr(String clientAddr) {
		this.clientAddr = clientAddr;
	}
	
	public String getStorageDirectoryPath() {
		return this.storageDirectoryPath;
	}
	
	public String getStorageNodeAddr() {
		return storageNodeAddr;
	}

	public void setStorageNodeAddr(String storageNodeAddr) {
		this.storageNodeAddr = storageNodeAddr;
	}
	
	public String getControllerAddr() {
		return this.controllerAddr;
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
	
	public int getMaxStorageValue() {
		return this.maxStorageValue;
	}

	public void setMaxStorageValue(int maxStorageValue) {
		this.maxStorageValue = maxStorageValue;
	}
}