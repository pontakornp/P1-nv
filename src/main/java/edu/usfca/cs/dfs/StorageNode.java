package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;

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
	private String storageNodeDirectoryPath;
	private int storageNodePort;
	private long availableStorageCapacity;
	private long maxStorageCapacity;
	private List<StorageMessages.ReplicaNode> replicaStorageNodes;
	
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
	
	public String getStorageNodeId() {
		return this.storageNodeId;
	}
	
	public String getStorageNodeAddr() {
		return this.storageNodeAddr;
	}
	
	public int getStorageNodePort() {
		return this.storageNodePort;
	}
	
	public String getControllerNodeAddr() {
		return this.controllerNodeAddr;
	}
	
	public int getControllerNodePort() {
		return this.controllerNodePort;
	}
	
	public long getAvailableStorageCapacity() {
		return this.availableStorageCapacity;
	}
	
	public long getMaxStorageCapacity() {
		return this.maxStorageCapacity;
	}

	public List<StorageMessages.ReplicaNode> getReplicaStorageNodes() {
		return this.replicaStorageNodes;
	}

	public void setReplicaStorageNodes(List<StorageMessages.ReplicaNode> replicaStorageNodes) {
		this.replicaStorageNodes = replicaStorageNodes;
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

	public void setAvailableStorageCapacity(long availableStorageCapacity) {
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

	private void setVariables(Config config) {
		this.storageNodeId = UUID.randomUUID().toString();
		this.storageNodeAddr = config.getStorageNodeAddr();
		this.storageNodePort = config.getStorageNodePort();
		this.storageNodeDirectoryPath = config.getStorageNodeDirectoryPath() + storageNodeId + '/';
		this.maxStorageCapacity = config.getMaxStorageCapacity();
		this.availableStorageCapacity = this.maxStorageCapacity;
		this.replicaStorageNodes = new ArrayList<StorageMessages.ReplicaNode>();
		
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
	public synchronized void registerNode() {
		StorageNode storageNode = StorageNode.getInstance();
		Thread thread = new Thread() {
			public void run() {
				try {
					EventLoopGroup workerGroup = new NioEventLoopGroup();
			        MessagePipeline pipeline = new MessagePipeline();
			        
			        logger.info("Registration initiated to controller: " + 
			        		storageNode.getControllerNodeAddr() + String.valueOf(storageNode.getControllerNodePort()));
			        Bootstrap bootstrap = new Bootstrap()
			            .group(workerGroup)
			            .channel(NioSocketChannel.class)
			            .option(ChannelOption.SO_KEEPALIVE, true)
			            .handler(pipeline);
			        
			        ChannelFuture cf = bootstrap.connect(storageNode.getControllerNodeAddr(), storageNode.getControllerNodePort());
			        cf.syncUninterruptibly();
			
			        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRegisterNodeRequest(StorageNode.getInstance());
			
			        Channel chan = cf.channel();
			        chan.writeAndFlush(msgWrapper);
			        logger.info("Registration message sent to controller");
			        chan.closeFuture().sync();
			        workerGroup.shutdownGracefully();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error("Registration of storage node failed. Controller connection establishment failed");
				}
			}
		};
		thread.start();
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
	        
	        logger.info("Heartbeat send to controller initiated: " + this.controllerNodeAddr + String.valueOf(this.controllerNodePort));
	        ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructHeartBeatRequest(StorageNode.getInstance());
	
	        Channel chan = cf.channel();
	        ChannelFuture write = chan.write(msgWrapper);
	        chan.flush();
	        write.syncUninterruptibly();
	        chan.closeFuture().sync();
	        workerGroup.shutdownGracefully();
	        logger.info("Heartbeat send to controller completed");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Heartbeat send of storage node failed. Controller connetion establishment failed");
		}
	}
	
	public void handleHeartBeats() {
		Thread thread = new Thread() {
			public void run() {
				while(true) {
					try {
						Thread.sleep(5000);
						logger.info("HeartbeatThread running");
						StorageNode.getInstance().sendHeartBeat();
			            logger.info("HeartbeatThread sleeping");
			        } catch (InterruptedException e) {
			            e.printStackTrace();
			        }
				}
			}
		};
		thread.start();
	}

	// To store chunk in a file,
	// 1. calculate Shannon Entropy of the files which is the maximum compression
	// if maximum compression is greater than 0.6, then compress the chunk data
	// else do not compress
	// 2. store the chunk
	// 3. do check sum if the it is corrupted or not
	// return true if the chunk is not corrupted, else return false
	public synchronized StorageMessages.StoreChunkRequest storeChunk(StorageMessages.StoreChunkRequest storeChunkRequest) {
		StorageMessages.Chunk chunk = storeChunkRequest.getChunk();
		String fileName = chunk.getFileName();
		int chunkId = chunk.getChunkId();
		byte[] chunkData = chunk.getData().toByteArray();
		
		boolean isClientInitiated = storeChunkRequest.getIsClientInitiated();
		StorageMessages.StorageNode previousStorageNode = storeChunkRequest.getStorageNode();
		
		boolean isNewChunk = storeChunkRequest.getIsNewChunk();
		boolean isPrimaryNode = false;
		boolean isPreviousVersion = storeChunkRequest.getFileExists();
		boolean isCompressed = false;
		
		StringBuilder filePathBuilder = new StringBuilder(); 
		filePathBuilder.append(fileName);
		filePathBuilder.append("_" + chunkId);
		String outputFileName = filePathBuilder.toString();
		
		File dir = null;
		if(isNewChunk && !isPreviousVersion) {
			if(isClientInitiated) {
				dir = new File(this.storageNodeDirectoryPath, this.storageNodeId);
				isPrimaryNode = true;
			}else {
				isPrimaryNode = false;
				dir = new File(this.storageNodeDirectoryPath, previousStorageNode.getStorageNodeId());
			}
			if (!dir.exists()) {
				dir.mkdirs();
				logger.info("Created new file directory on storage node: " + dir.toString());
			}
		}else {
			// This is the case for false positives or multiple versions
			// Finds previous file location in base path
			File basePath = new File(this.storageNodeDirectoryPath);
			for(File subdirectory: basePath.listFiles()) {
				if(subdirectory.isDirectory()) {
					File oldVersionFile = new File(subdirectory.getAbsolutePath(), outputFileName);
					if(oldVersionFile.exists()) {
						dir = subdirectory;
						isPreviousVersion = true;
						logger.info("Old file version detected");
						if(subdirectory.getName()==this.storageNodeId) {
							isPrimaryNode = true;
							logger.info("Old file version detected as primary");
						}else {
							logger.info("Old file version detected as replica");
						}
					}
				}
			}
		}
		
		if(dir!=null) {
			// Do compression iff client is initiating the save
			if(isClientInitiated) {
				double entropyBits = Entropy.calculateShannonEntropy(chunkData);
				double maximumCompression = 1 - (entropyBits/8);
				if (maximumCompression > 0.6) {
					chunkData = CompressDecompress.compress(chunkData);
					if (chunkData == null) {
						logger.error("Fails to compress chunk");
						return null;
					}
					chunk = chunk.toBuilder().setData(ByteString.copyFrom(chunkData)).build();
					isCompressed = true;
				}
			}
			
			try {
				File outputFile = new File(dir.toString(), outputFileName);
				File metaoutputFile = new File(dir.toString(), outputFileName+".meta");
				String checksum = CheckSum.checkSum(chunkData);
				
				logger.info("File getting saved at path: " + outputFile.toString());
				outputFile.createNewFile();
				RandomAccessFile aFile = new RandomAccessFile(outputFile, "rw");
				aFile.write(chunkData, 0, chunkData.length);
				aFile.close();
				
				ChunkMetaData chunkMetaData = new ChunkMetaData(
					chunk.getFileName(), chunk.getFileSize(), 
					chunk.getChunkId(), chunk.getChunkSize(),
					chunk.getMaxChunkNumber(), checksum, isCompressed);
				
				Gson gson = new Gson();
				Writer writer = new FileWriter(metaoutputFile);
				gson.toJson(chunkMetaData, writer);
				writer.flush();
		        writer.close();
				
				this.setAvailableStorageCapacity(this.getAvailableStorageCapacity()-outputFile.length());
				if(isPrimaryNode && chunk.getPrimaryCount()==0) {
					chunk = chunk.toBuilder().setPrimaryCount(chunk.getPrimaryCount()+1).build();
				}else {
					chunk = chunk.toBuilder().setReplicaCount(chunk.getPrimaryCount()+1).build();
				}
				
				// TODO: Create metachunk file and save to file directory
				storeChunkRequest = storeChunkRequest.toBuilder()
						.setChunk(chunk)
						.setIsClientInitiated(false)
						.setIsNewChunk(isNewChunk)
						.setFileExists(isPreviousVersion)
						.build();
				
				if(!isPreviousVersion) {
					logger.info("Previous version do not exists. Replicating chunks and updating controller");
					//TODO: this needs to be executed in separate thread each
					this.storeChunkOnReplica(storeChunkRequest);
					this.updateControllerOnChunkSave(chunk);
				}
				return storeChunkRequest;
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("There is a problem when writing stream to file");
				return null;
			}
		}else {
			logger.info("False positive detected for file. Doing nothing" + fileName);
			return storeChunkRequest;
		}
	}
	
	public void updateControllerOnChunkSave(StorageMessages.Chunk chunk) {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
	        MessagePipeline pipeline = new MessagePipeline();
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        logger.info("Chunk save update send to controller initiated: " + this.controllerNodeAddr + String.valueOf(this.controllerNodePort));
	        ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructStoreChunkControllerUpdateRequest(chunk, StorageNode.getInstance());
	
	        Channel chan = cf.channel();
	        ChannelFuture write = chan.write(msgWrapper);
	        chan.flush();
	        write.syncUninterruptibly();
	        chan.closeFuture().sync();
	        workerGroup.shutdownGracefully();
	        logger.info("Heartbeat send to controller completed");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Heartbeat send of storage node failed. Controller connetion establishment failed");
		}
		
	}

	/**
	 * store chunks in all of the storage node's replicas
	 * @param storeChunkRequest
	 */
	public boolean storeChunkOnReplica(StorageMessages.StoreChunkRequest storeChunkRequest) {
		StorageMessages.MessageWrapper message = 
				HDFSMessagesBuilder.constructStoreChunkRequest(
						storeChunkRequest.getChunk(), 
						storeChunkRequest.getStorageNode(), 
						storeChunkRequest.getIsClientInitiated(), 
						storeChunkRequest.getIsNewChunk());
		for (StorageMessages.ReplicaNode replica: this.replicaStorageNodes) {
			boolean isReplicated = storeChunkOnReplicaHelper(message, replica);
			if (!isReplicated) {
				return false;
			}
		}
		return true;
	}


	private boolean storeChunkOnReplicaHelper(MessageWrapper message, StorageMessages.ReplicaNode storageNode) {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
			MessagePipeline pipeline = new MessagePipeline();

			logger.info("Connection initiated to replica node replica: " 
					+ storageNode.getStorageNodeAddr() + String.valueOf(storageNode.getStorageNodePort()));
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
			chan.closeFuture().sync();
			workerGroup.shutdownGracefully();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("replicate chunk failed. Replicated storage node connection establishment failed");
			return false;
		}
	}

	// retrieve chunk from a file
	public synchronized StorageMessages.MessageWrapper retrieveChunk(String fileName, int chunkNumber) {
		String file_key = fileName + '_' + chunkNumber;
		// Finds file location in base path
		File basePath = new File(this.storageNodeDirectoryPath);
		File filePath = null;
		for(File subdirectory: basePath.listFiles()) {
			if(subdirectory.isDirectory()) {
				filePath = new File(subdirectory.getAbsolutePath(), file_key);
				if(filePath.exists()) {
					logger.info("File existence detected for chunk retreiveal on Node with storage id : " + this.storageNodeId);
				}
				break;
			}
		}
		
		if (filePath ==null) {
			logger.info("False positive detected for file retreival. No file found on storage node");
			return null;
		}else {
			String metaPath = filePath + ".meta";
			try {
				ChunkMetaData chunkMetaData = new ChunkMetaData();
				chunkMetaData.setChunkMetaDataWithFilePath(metaPath);
				
				byte[] chunkData = Files.readAllBytes(Paths.get(filePath.getAbsolutePath()));
				//if chunk is compressed, need to decompress the byte array data before returning it
				// else return the byte array data right away
				if (chunkMetaData.isCompressed) {
					chunkData = CompressDecompress.decompress(chunkData);
				}
				StorageMessages.MessageWrapper msgWrapper = HDFSMessagesBuilder.constructChunkFromFile(chunkMetaData, chunkData);
				return msgWrapper;
			} catch (IOException e) {
				logger.error("File not exist");
				return null;
			}
		}
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

	public synchronized void getChunk(String storageNodeAddr, int storageNodePort, String fileName, int chunkNumber, String storageNodeId) {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
			MessagePipeline pipeline = new MessagePipeline();

			logger.info("Get chunk from storage node: " + storageNodeAddr + String.valueOf(storageNodePort));
			Bootstrap bootstrap = new Bootstrap()
					.group(workerGroup)
					.channel(NioSocketChannel.class)
					.option(ChannelOption.SO_KEEPALIVE, true)
					.handler(pipeline);

			ChannelFuture cf = bootstrap.connect(storageNodeAddr, storageNodePort);
			cf.syncUninterruptibly();

			MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRecoverChunkRequest(fileName, chunkNumber, storageNodeId);

			Channel chan = cf.channel();
			ChannelFuture write = chan.writeAndFlush(msgWrapper);
			logger.info("Get chunk from storage node");
			chan.closeFuture().sync();
			workerGroup.shutdownGracefully();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Get Chunk from storage node failed. Storage Node connection establishment failed");
		}
	}

	public synchronized void recoverChunk(String fileName, int chunkNumber, String recoverStorageNodeId) {
		File basePath = new File(this.storageNodeDirectoryPath);
		File filePath = null;
		// if it's a primary, recover the chunk from the replica with subfolder same as the primary node id
		if(this.storageNodeId == recoverStorageNodeId) {
			for(StorageMessages.ReplicaNode replicaNode: replicaStorageNodes) {
				// contact replica with address and port to get the chunk from it
				//form recover chunk request
				String addr = replicaNode.getStorageNodeAddr();
				int port = replicaNode.getStorageNodePort();
				getChunk(addr, port, fileName, chunkNumber, recoverStorageNodeId);
			}
		} else {
			// if it's a replica, recover the chunk from the its primary with storage node id and subfolder as the replica id
			// contact controller to get the address and port of the storage
			Controller controller = Controller.getInstance();
			// call a method in controller to get the storage node object
			List<StorageMessages.StorageNode> activateStorageNodes = controller.getActiveStorageNodes();
			for(StorageMessages.StorageNode storageNode: activateStorageNodes) {
				if(storageNode.getStorageNodeId().equals(recoverStorageNodeId)) {
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

						StorageMessages.Chunk chunk = StorageMessages.Chunk.newBuilder()
								.setChunkId(chunkNumber)
								.setFileName(fileName)
								.build();

						StorageMessages.MessageWrapper msgWrapper = HDFSMessagesBuilder.constructGetNodesFromController(chunk, storageNodeId);

						Channel chan = cf.channel();
						ChannelFuture write = chan.writeAndFlush(msgWrapper);
						logger.info("Request controller for active nodes");
						chan.closeFuture().sync();
						workerGroup.shutdownGracefully();
					} catch (Exception e) {
						e.printStackTrace();
						logger.error("Fail to request to controller");
					}

				}
			}

		}
	}

	public synchronized StorageMessages.MessageWrapper retrieveRecoverChunk(String fileName, int chunkNumber, String storageNodeId) {
		File basePath = new File(this.storageNodeDirectoryPath);
		File filePath = null;

		// look for subdirectory name storageNodeId
		File subdirectory = new File(basePath.getAbsolutePath() + "/" + storageNodeId);
		String file_key = fileName + "_" + chunkNumber;
		filePath = new File(subdirectory.getAbsolutePath(), file_key);
		if(filePath.exists()) {
			logger.info("chunk to be recovered is found");
			try {
				byte[] chunkData = Files.readAllBytes(Paths.get(filePath.getAbsolutePath()));
				String metaPath = filePath.getAbsolutePath()+".meta";
				ChunkMetaData chunkMetaData = new ChunkMetaData();
				chunkMetaData.setChunkMetaDataWithFilePath(metaPath);
				StorageMessages.MessageWrapper msgWrapper = HDFSMessagesBuilder.constructChunkFromFile(chunkMetaData, chunkData);
				return msgWrapper;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}

	public synchronized void storeRecoverChunk(StorageMessages.Chunk chunkMsg, String storageNodeId) {

		try {

			String outputFileName = chunkMsg.getFileName() + "_" + chunkMsg.getChunkId();
			File dir = new File(this.storageNodeDirectoryPath + "/" + storageNodeId + "/", outputFileName);
			File outputFile = new File(dir.toString(), outputFileName);
			FileOutputStream outputStream = new FileOutputStream(outputFile);
			outputStream.write(chunkMsg.getData().toByteArray());
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
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
		try {
			//storageNode.handleHeartBeats();
			storageNode.start();
		}catch (Exception e){
			System.out.println("Unable to start storage node");
			e.printStackTrace();
		}
	}
}
