package edu.usfca.cs.dfs.config;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

/**
 * @author pontakornp
 * Manages config file of non-blocking file transfer
 */
public class Config {
	
	static Logger logger = LogManager.getLogger(Config.class);
	
	private String clientAddr;
	private String clientDirectoryPath;
	
	private String controllerNodeAddr;
	private int controllerNodePort;
	
	private String storageNodeAddr;
	private int storageNodePort;
	private String storageDirectoryPath;
	private int maxStorageCapacity;

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
			System.out.println(config.toString());
			this.clientAddr = config.clientAddr;
			
			this.controllerNodeAddr = config.controllerNodeAddr;
			this.controllerNodePort = config.controllerNodePort;
			
			this.storageNodeAddr = config.storageNodeAddr;
			this.storageNodePort = config.storageNodePort;
			this.storageDirectoryPath = config.storageDirectoryPath;
			this.maxStorageCapacity = config.maxStorageCapacity;
			
			this.chunkSize = config.chunkSize;
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
	
	public String getControllerNodeAddr() {
		return this.controllerNodeAddr;
	}

	public void setControllerNodeAddr(String controllerNodeAddr) {
		this.controllerNodeAddr = controllerNodeAddr;
	}
	
	public int getControllerNodePort() {
		return controllerNodePort;
	}

	public void setControllerNodePort(int controllerNodePort) {
		this.controllerNodePort = controllerNodePort;
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
	
	public int getMaxStorageCapacity() {
		return this.maxStorageCapacity;
	}

	public void setMaxStorageCapacity(int maxStorageCapacity) {
		this.maxStorageCapacity = maxStorageCapacity;
	}
}