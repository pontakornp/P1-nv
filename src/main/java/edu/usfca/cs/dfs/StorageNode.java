package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.StorageMessages.MessageWrapper;
import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.util.CheckSum;
import edu.usfca.cs.dfs.util.CompressDecompress;
import edu.usfca.cs.dfs.util.Entropy;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class StorageNode {

	static Logger logger = LogManager.getLogger(StorageNode.class);
	
	private String storageNodeId;
	private String storageNodeAddr;
	private String storageNodeDirectoryPath; // storageNodeAddr + storageNodeAddr + '/'
	private int storageNodePort;
	private int currentStorageValue;
	private int maxStorageValue;
	
	private ArrayList<String> replicationNodeIds;
	private HashMap<String, Chunk> metaDataMap;
	
	private String controllerNodeAddr;
	private int controllerNodePort;
	
	private static StorageNode storageNodeInstance = null; 

	private StorageNode() {
		
	}
	
	public static StorageNode getInstance() { 
        if (storageNodeInstance == null) 
        	storageNodeInstance = new StorageNode(); 
  
        return storageNodeInstance; 
    } 
	
	private void setVariables(Config config) {
		this.storageNodeId = UUID.randomUUID().toString();
		this.storageNodeAddr = config.getStorageNodeAddr();
		this.storageNodePort = config.getStorageNodePort();
		this.storageNodeDirectoryPath = storageNodeAddr + storageNodePort;
		this.currentStorageValue = 0;
		this.maxStorageValue = config.getMaxStorageValue();
		this.replicationNodeIds = new ArrayList<String>();
		this.metaDataMap = new HashMap<String, Chunk>();
		
		this.controllerNodeAddr = config.getControllerNodeAddr();
		this.controllerNodePort = config.getControllerNodePort();
		System.out.println("Storage Node config updated. Storage Node Id: "+ this.storageNodeId);
	}
	
	/*
     * This will use protobuf message to create the chunk message
     * This will be used by client to send to storageNode to save particular chunk
     */
    public MessageWrapper constructRegisterNodeMessage() {
    	
    	StorageMessages.StorageNode registerNodeMessage
        = StorageMessages.StorageNode.newBuilder()
            .setStorageNodeId(this.storageNodeId)
            .setStorageNodeAddr(this.storageNodeAddr)
            .setStorageNodePort(this.storageNodePort)
            .setCurrentStorageValue(this.currentStorageValue)
            .setMaxStorageValue(this.maxStorageValue)
            .build();
    	
    	StorageMessages.MessageWrapper msgWrapper =
                StorageMessages.MessageWrapper.newBuilder()
                    .setStorageNodeMsg(registerNodeMessage)
                    .build();
    	
    	return msgWrapper;
    }

	/*
	 * This send a request to controller to register the node onto the controller
	 * @request parameters : None
	 * @return type: None
	 */
	public void registerNode() {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
	        MessagePipeline pipeline = new MessagePipeline();
	        
	        System.out.println("Registration initiated to controller");
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        System.out.println(this.controllerNodeAddr+String.valueOf(this.controllerNodePort));
	        
	        ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = this.constructRegisterNodeMessage();
	
	        Channel chan = cf.channel();
	        ChannelFuture write = chan.write(msgWrapper);
	        chan.flush();
	        write.syncUninterruptibly();
	
	        /* Don't quit until we've disconnected: */
	        System.out.println("Registration message sent to controller");
	        workerGroup.shutdownGracefully();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Registration of storage node failed. Controller connetion establishment failed");
		}
		
	}

	public String getStorageNodeId() {
		return this.storageNodeId;
	}
	
	public String getStorageNodeAddr() {
		return this.storageNodeAddr;
	}
	
	public int getStorageNodePort() {
		return this.storageNodePort;
	}
	
	public int getCurrentStorageValue() {
		return this.currentStorageValue;
	}
	
	public int getMaxStorageValue() {
		return this.maxStorageValue;
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
		File dir = new File(this.storageNodeDirectoryPath);
		if (!dir.exists()) {
			dir.mkdir();
			logger.info("Created new directory");
		}
		// store the chunk in a file
		try {
			OutputStream outputStream = new FileOutputStream(this.storageNodeDirectoryPath + fileName);
			outputStream.write(data);
			byte[] storedChunkData = Files.readAllBytes(Paths.get(this.storageNodeDirectoryPath + fileName));
			// check sum if the file is corrupted or not
			if (!isFileCorrupted(storedChunkData, originalCheckSum)) {
				// add chunk to the meta data map
				Chunk chunkObj = new Chunk(originalCheckSum, isCompressed, chunkNumber, chunkData.length);
				String chunkKey = fileName + '_' + chunkNumber;
				metaDataMap.put(chunkKey, chunkObj);
				logger.info("add chunk");
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			System.out.println("There is a problem when writing stream to file.");
			return false;
		}
	}

	// get number of chunks
	public synchronized Integer getChunkCount(String fileName) {
		return 0;
	}
	// get chunk location
	public synchronized String getChunkLocation(String fileName, Integer chunkNumber) {
		String filePath = this.storageNodeDirectoryPath + fileName + '_' + chunkNumber;
		return filePath;
	}

	// retrieve chunk from a file
	public synchronized byte[] retrieveChunk(String fileName, Integer chunkNumber) {
		// 1. get chunk location
		String filePath = getChunkLocation(fileName, chunkNumber);
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
	
	public void start() throws IOException, InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline();
        
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
              .channel(NioServerSocketChannel.class)
              .childHandler(pipeline)
              .option(ChannelOption.SO_BACKLOG, 128)
              .childOption(ChannelOption.SO_KEEPALIVE, true);
 
            ChannelFuture f = b.bind(this.storageNodePort).sync();
            System.out.println("Storage Node started at port: " + String.valueOf(this.storageNodePort));
            this.registerNode();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
	
	public static void main(String args[]) {
		String configFileName;
		if(args.length>0) {
    		configFileName= args[0];
    	}else {
    		configFileName = "config.json";
    	}
		Config config = new Config(configFileName);
		StorageNode storageNode = StorageNode.getInstance();
		storageNode.setVariables(config);
		
		try {
			storageNode.start();
		}catch (Exception e){
			System.out.println("Unable to start storage node");
			e.printStackTrace();
		}
	}
}
