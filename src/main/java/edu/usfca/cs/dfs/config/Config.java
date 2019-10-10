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
	private String clientDirectoryPath;
	private String storageDirectoryPath;
	private String controllerHostName;
	private int chunkSize;

	/**
	 * Set variables from the config.json file and assign them to variables in this class.
	 */
	public boolean setVariables() {
		Config config = new Config();
		try {
			JsonReader jsonReader = new JsonReader(new FileReader("config.json"));
			Gson gson = new Gson();
			config = gson.fromJson(jsonReader, Config.class);
			this.clientDirectoryPath = config.clientDirectoryPath;
			this.storageDirectoryPath = config.storageDirectoryPath;
			this.controllerHostName = config.controllerHostName;
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

	public void setClientDirectoryPath(String clientDirectoryPath) {
		this.clientDirectoryPath = clientDirectoryPath;
	}
	
	public String getstorageDirectoryPath() {
		return this.storageDirectoryPath;
	}
	
	public void setStorageDirectoryPath(String storageDirectoryPath) {
		this.storageDirectoryPath = storageDirectoryPath;
	}
	
	public String getControllerHostName() {
		return this.controllerHostName;
	}

	public void setControllerHostName(String controllerHostName) {
		this.controllerHostName = controllerHostName;
	}
	
	public int getChunkSize() {
		return chunkSize;
	}
	
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}
}