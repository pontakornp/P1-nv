package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
	private String storageNodeDirectoryPath; // storageNodeAddr + storageNodeId + '/'
	private int storageNodePort;
	private long availableStorageCapacity;
	private long maxStorageCapacity;
	
	private List<String> replicationNodeIds;
	private List<StorageNode> replicatedStorageNodeObjs;
	
	private String controllerNodeAddr;
	private int controllerNodePort;
	
	private static StorageNode storageNodeInstance = null; 

	public StorageNode() {
		
	}
	
	public static StorageNode getInstance() { 
        if (storageNodeInstance == null) 
        	storageNodeInstance = new StorageNode(); 
        return storageNodeInstance; 
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
	
	public long getAvailableStorageCapacity() {
		return this.availableStorageCapacity;
	}
	
	public long getMaxStorageCapacity() {
		return this.maxStorageCapacity;
	}

	public List<String> getReplicationNodeIds() {
		return this.replicationNodeIds;
	}

	public void setReplicationNodeIds(List<String> replicationNodesIdList) {
		this.replicationNodeIds = replicationNodesIdList;
	}

	public List<StorageNode> getReplicatedNodeObjs() {return this.replicatedStorageNodeObjs;}

	public void setReplicatedStorageNodeObjs(List<StorageNode> replicatedStorageNodeObjs) {
		this.replicatedStorageNodeObjs = replicatedStorageNodeObjs;
	}

	public void setStorageNodeId(String storageNodeId) {
		this.storageNodeId = storageNodeId;
	}

	public void setStorageNodeAddr(String storageNodeAddr) {
		this.storageNodeAddr = storageNodeAddr;
	}

	public void setStorageNodeDirectoryPath(String storageNodeDirectoryPath) {
		this.storageNodeDirectoryPath = storageNodeDirectoryPath;
	}

	public void setStorageNodePort(int storageNodePort) {
		this.storageNodePort = storageNodePort;
	}

	public void setAvailableStorageCapacity(int availableStorageCapacity) {
		this.availableStorageCapacity = availableStorageCapacity;
	}

	public void setMaxStorageCapacity(int maxStorageCapacity) {
		this.maxStorageCapacity = maxStorageCapacity;
	}

	public void setControllerNodeAddr(String controllerNodeAddr) {
		this.controllerNodeAddr = controllerNodeAddr;
	}

	public void setControllerNodePort(int controllerNodePort) {
		this.controllerNodePort = controllerNodePort;
	}

	public static void setStorageNodeInstance(StorageNode storageNodeInstance) {
		StorageNode.storageNodeInstance = storageNodeInstance;
	}

	/**
	 * store chunks in all of the storage node's replicas
	 * @param chunk
	 */
	public boolean storeChunkOnReplica(StorageMessages.Chunk chunk) {
		StorageMessages.MessageWrapper message = HDFSMessagesBuilder.constructStoreChunkRequest(chunk, false);
		for (StorageNode replica: this.replicatedStorageNodeObjs) {
			boolean isReplicated = storeChunkOnReplicaHelper(message, replica);
			if (!isReplicated) {
				//sleep for sometime and retry
			}
		}
		return true;
	}


	private boolean storeChunkOnReplicaHelper(MessageWrapper message, StorageNode storageNode) {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
			MessagePipeline pipeline = new MessagePipeline();

			logger.info("Connection initiated to storage node replica: " + this.controllerNodeAddr + String.valueOf(this.controllerNodePort));
			Bootstrap bootstrap = new Bootstrap()
					.group(workerGroup)
					.channel(NioSocketChannel.class)
					.option(ChannelOption.SO_KEEPALIVE, true)
					.handler(pipeline);

			ChannelFuture cf = bootstrap.connect(storageNode.getStorageNodeAddr(), storageNode.getStorageNodePort());
			cf.syncUninterruptibly();
			Channel chan = cf.channel();
			ChannelFuture write = chan.write(message);
			chan.flush();
			write.syncUninterruptibly();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("replicate chunk failed. Replicated storage node connection establishment failed");
			return false;
		}
	}
	
	private void setVariables(Config config) {
		this.storageNodeId = UUID.randomUUID().toString();
		this.storageNodeAddr = config.getStorageNodeAddr();
		this.storageNodePort = config.getStorageNodePort();
		this.storageNodeDirectoryPath = config.getStorageNodeDirectoryPath() + storageNodeId + '/';
		this.maxStorageCapacity = config.getMaxStorageCapacity();
		this.availableStorageCapacity = this.maxStorageCapacity;
		
		this.replicationNodeIds = new ArrayList<String>();
		
		this.controllerNodeAddr = config.getControllerNodeAddr();
		this.controllerNodePort = config.getControllerNodePort();
		System.out.println("Storage Node config updated. Storage Node Id: "
			+ this.storageNodeId +  " StorageNode Size: " + this.availableStorageCapacity);
	}
    
    public void updateValuesFromProto(StorageMessages.StorageNode storageNodeMsg) {
		this.storageNodeId = storageNodeMsg.getStorageNodeId();
		this.storageNodeAddr = storageNodeMsg.getStorageNodeAddr();
		this.storageNodePort = storageNodeMsg.getStorageNodePort();
		this.availableStorageCapacity = storageNodeMsg.getAvailableStorageCapacity();
		this.maxStorageCapacity = storageNodeMsg.getMaxStorageCapacity();
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
	        
	        logger.info("Registration initiated to controller: " + this.controllerNodeAddr + String.valueOf(this.controllerNodePort));
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRegisterNodeRequest(StorageNode.getInstance());
	
	        Channel chan = cf.channel();
	        ChannelFuture write = chan.writeAndFlush(msgWrapper);
	        
	        logger.info("Registration message sent to controller");
	        chan.closeFuture().sync();
	        workerGroup.shutdownGracefully();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Registration of storage node failed. Controller connection establishment failed");
		}
	}
	
	/*
	 * This send a request to controller to register the node onto the controller
	 * @request parameters : None
	 * @return type: None
	 */
	// send heartbeat to controller to inform that storage node is still available
	private void sendHeartBeat() {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
	        MessagePipeline pipeline = new MessagePipeline();
	        
	        System.out.println("HeartBeat initiated to controller");
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        System.out.println(this.controllerNodeAddr+String.valueOf(this.controllerNodePort));
	        ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructHeartBeatRequest(StorageNode.getInstance());
	
	        Channel chan = cf.channel();
	        ChannelFuture write = chan.write(msgWrapper);
	        chan.flush();
	        write.syncUninterruptibly();
	        chan.closeFuture().sync();
	        workerGroup.shutdownGracefully();
	        System.out.println("HeartBeat message sent to controller");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Registration of storage node failed. Controller connetion establishment failed");
		}
	}
	
	public void handleHeartBeats() {
		Thread thread = new Thread() {
			public void run() {
				while(true) {
					try {
						System.out.println("HeartbeatThread running");
						StorageNode.getInstance().sendHeartBeat();
						System.out.println("HeartbeatThread sleeping");
			            Thread.sleep(5000);
			        } catch (InterruptedException e) {
			            e.printStackTrace();
			        }
				}
			}
		};
		thread.start();
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
	public boolean storeChunk(String fileName, int chunkId, byte[] chunkData, String originalCheckSum) {
		// calculate Shannon Entropy
		double entropyBits = Entropy.calculateShannonEntropy(chunkData);
		double maximumCompression = 1 - (entropyBits / 8);
		byte[] data = chunkData;
		// if maximum compression is greater than 0.6, then compress the chunk data
		// else do not compress
		// then store the compress or uncompressed chunk data in a file
		StringBuilder filePathBuilder = new StringBuilder();
		filePathBuilder.append(this.storageNodeId + "/");
		filePathBuilder.append(fileName);
		filePathBuilder.append("_" + chunkId);
		if (maximumCompression > 0.6) {
			data = CompressDecompress.compress(chunkData);
			filePathBuilder.append("_compressed");
			if (data == null) {
				logger.error("Fails to compress chunk");
				return false;
			}
		}
		filePathBuilder.append("_" + originalCheckSum);
		String filePath = filePathBuilder.toString();
		// create directory
		File dir = new File(this.storageNodeId);

		if (!dir.exists()) {
			dir.mkdir();
			logger.info("Created new directory");
		}
		try {
			OutputStream outputStream = new FileOutputStream(filePath);
			outputStream.write(data);
			byte[] storedChunkData = Files.readAllBytes(Paths.get(filePath));
			if (!isFileCorrupted(storedChunkData, originalCheckSum)) {
				logger.info("Chunk is added successfully");
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			logger.error("There is a problem when writing stream to file");
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
		File file = new File(filePath);
		if (file.exists()) {
			return filePath;
		}
		String compressedFilePath = filePath + "_compressed";
		File compressedFile = new File(compressedFilePath);
		if (compressedFile.exists()) {
			return compressedFilePath;
		}
		return null;
	}

	// retrieve chunk from a file
	public synchronized byte[] retrieveChunk(String fileName, int chunkId) {
		// 1. get chunk location
		String filePath = getChunkLocation(fileName, chunkId);
		// 2. if file does not exist returns null
		if (filePath == null) {
			return null;
		}
		boolean isCompressed = false;
		if (filePath.contains("_compressed")) {
			isCompressed = true;
		}
		// 3. if chunk is compressed, need to decompress the byte array data before returning it
		// else return the byte array data right away
		try {
			byte[] chunkData = Files.readAllBytes(Paths.get(filePath));
			// if chunk is compressed
			if (isCompressed) {
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

		logger.info(storageNode.storageNodeDirectoryPath);
		try {
			storageNode.sendHeartBeat();
			storageNode.start();
		}catch (Exception e){
			System.out.println("Unable to start storage node");
			e.printStackTrace();
		}


		String checkSum = "098f6bcd4621d373cade4e832627b4f6";
		try {
			byte[] temp = Files.readAllBytes(Paths.get("/Users/pontakornp/Documents/projects/bigdata/P1-nv/test.jpg"));
			StorageNode sn = new StorageNode();
			System.out.println(sn.storeChunk("test2.jpg", 1, temp, checkSum));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
