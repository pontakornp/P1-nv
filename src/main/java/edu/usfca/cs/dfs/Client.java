package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages.MessageWrapper;
import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {
	
	static Logger logger = LogManager.getLogger(Client.class);
	private static int chunkSize; // This is chunk size in bytes
	private static String controllerNodeAddr;
	private static Integer controllerNodePort;
	private static String fileDestinationPath;
	private static ConcurrentHashMap<String, StorageMessages.Chunk> chunkMapPut = new ConcurrentHashMap<String, StorageMessages.Chunk>();
	private static ConcurrentHashMap<String, StorageMessages.Chunk> chunkMapGet = new ConcurrentHashMap<String, StorageMessages.Chunk>();
	private static ConcurrentHashMap<String, StorageMessages.Chunk> chunkMap = new ConcurrentHashMap<String, StorageMessages.Chunk>();
	private static Client clientInstance;
	private static int MAX_REPLICAS = 2;


	public Client() {

    }

	public static Client getInstance() {
		if (clientInstance == null){
			System.out.println("New controller instance instantiated");
			clientInstance = new Client();
		}
		return clientInstance;
	}

    private synchronized void setVariables(Config config) {
		this.chunkSize = config.getChunkSize();
		Client.controllerNodeAddr = config.getControllerNodeAddr();
		Client.controllerNodePort = config.getControllerNodePort();
		Client.fileDestinationPath = config.getClientDirectoryPath();
		logger.info("Client Node config updated.");
	}
    
    /*
     * This sends a request to controller to get list of storage nodes
     * to save for each chunk. Opens a channel to controller with 
     * fileName, chunkId, chunksize
     */
    public synchronized void sendFile(String filePath) {
    	File file = new File(filePath);
    	if (!file.exists()) {
    		logger.error("File with the given path: " +   filePath +  " does not exists");
    		return;
    	}
    	this.saveFileChunks(file);
    }
    
    /*
     * Calculates the maximum chunk number for a given file
     * Gets the chunkMappings for maxChunkNumber of a file
     */
    private void saveFileChunks(File file) {
    	try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
	        MessagePipeline pipeline = new MessagePipeline();
	        
	        logger.info("Save File initiated to controller: " + Client.controllerNodeAddr + String.valueOf(Client.controllerNodePort));
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        ChannelFuture cf = bootstrap.connect(Client.controllerNodeAddr, Client.controllerNodePort);
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructGetStorageNodesForChunksRequest(file, this.chunkSize);
	        Channel chan = cf.channel();
	        ChannelFuture write = chan.write(msgWrapper);
	        chan.flush();
	        write.syncUninterruptibly();
	        logger.info("Save File Chunks initial request sent to controller");
	        chan.closeFuture().sync();
	        workerGroup.shutdownGracefully();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("File Existence Check failed. Controller connection establishment failed");
		}
    }
    
    public static void getActiveNodeList() {
    	try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
	        MessagePipeline pipeline = new MessagePipeline();
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        ChannelFuture cf = bootstrap.connect(Client.controllerNodeAddr, Client.controllerNodePort);
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructGetActiveStorageNodeListRequest();
	        Channel chan = cf.channel();
	        ChannelFuture write = chan.write(msgWrapper);
	        chan.flush();
	        write.syncUninterruptibly();
	        logger.info("Get Active Nodes request sent to controller");
	        chan.closeFuture().sync();
	        workerGroup.shutdownGracefully();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("File Existence Check failed. Controller connection establishment failed");
		}
    }
    

    public static void retrieveFile(List<StorageMessages.ChunkMapping> chunkMappings) {
    	logger.info("got the chunk mapping yes!");
    	logger.info("Chunk mapping size: " + chunkMappings.size());
		for(StorageMessages.ChunkMapping chunkMapping: chunkMappings) {
			List<StorageMessages.StorageNode> storageNodeList = chunkMapping.getStorageNodeObjsList();
			StorageMessages.Chunk chunk = chunkMapping.getChunk();
			String fileName = chunk.getFileName();
			int chunkId = chunk.getChunkId();
			logger.info("got the chunk with file name: " + fileName + " chunkId: " + chunkId);
			// retrieve chunk from storage node and store it
			retrieveChunk(storageNodeList, fileName, chunkId);
		}
	}
	/**
	 * Client contacts storage nodes to get each chunk one by one, and store the chunk in the chunk mapping data structure
	 * @param storageNodeList
	 * @param fileName
	 * @param chunkId
	 */
	public static void retrieveChunk(List<StorageMessages.StorageNode> storageNodeList, String fileName, int chunkId) {
		logger.info("client retrieves chunk from storage node");
		for(StorageMessages.StorageNode storageNode: storageNodeList) {
			String addr = storageNode.getStorageNodeAddr();
			int port = storageNode.getStorageNodePort();
			logger.info("storage addr: " + addr + " storage port: " + port);
			StorageMessages.MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRetrieveChunkRequest(fileName, chunkId);
			String initiateMsg = "Retrieve Chunk initiated to storageNode: " + storageNode.getStorageNodeAddr() + "/:" + String.valueOf(storageNode.getStorageNodePort());
			String successMsg = "Retrieve Chunk completed at storageNode";
			String failMsg = "Retrieve Chunk failed. Storage node connection establishment failed";
			sendMsgWrapperToChannelFutureTemplate(addr, port, msgWrapper, initiateMsg, successMsg, failMsg);
			// store the chunk in the chunk mapping data structure
		}
	}

	public static void addChunkToChunkMap(String fileName, int chunkId, StorageMessages.Chunk chunkMsg) {
		Client.chunkMap.put(fileName + "_" + chunkId, chunkMsg);
	}

	public static void writeToFile(StorageMessages.Chunk chunk) {
		String fileName = chunk.getFileName();
		int chunkId = chunk.getChunkId();
		int chunkSize = chunk.getChunkSize();
		long fileSize = chunk.getFileSize();
		byte[] data = chunk.getData().toByteArray();
		logger.info("Writing to file for parameters: fileName: " + 
				fileName +  " chunkid: " + chunkId + " chunksize: " + chunkSize + " Filesize:  " + fileSize);
		
		File basePath = new File(Client.fileDestinationPath);
		if(!basePath.exists()) {
			basePath.mkdirs();
		}
		File outputFilePath = new File(Client.fileDestinationPath, fileName);
		logger.info(outputFilePath.getAbsolutePath());
		try {
			if(!outputFilePath.exists()) {
				outputFilePath.createNewFile();
			}
			RandomAccessFile aFile = new RandomAccessFile(outputFilePath, "rw");
			aFile.seek(chunkId*Client.chunkSize);
			aFile.write(data, 0, chunkSize);
			aFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Fail to write file");
		}
	}
	// This makes a request to controller for getting list of storage nodes containing chunkzero
    public static void retrieveFileRequestToController(String fileName, int maxChunkNumber) {
        try {
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            MessagePipeline pipeline = new MessagePipeline();

            logger.info("Retrieve File initiated to controller: " + controllerNodeAddr + String.valueOf(controllerNodePort));
            Bootstrap bootstrap = new Bootstrap()
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(pipeline);

            ChannelFuture cf = bootstrap.connect(Client.controllerNodeAddr, Client.controllerNodePort);
            cf.syncUninterruptibly();

            MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRetrieveFileRequest(fileName, maxChunkNumber);

            Channel chan = cf.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.syncUninterruptibly();
            logger.info("Retrieve File Chunks initial request sent to controller");
            chan.closeFuture().sync();
            workerGroup.shutdownGracefully();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("File Existence Check failed. Controller connection establishment failed");
        }
    }

    
    private static StorageMessages.Chunk updateChunkWithFileData(StorageMessages.Chunk chunk) {
    	String filePath = chunk.getFileAbsolutePath();
    	int chunkId = chunk.getChunkId();
    	int chunkSize = chunk.getChunkSize();
    	
    	System.out.println("Chunk update with file data initiated");
    	RandomAccessFile aFile;
		try {
			aFile = new RandomAccessFile(filePath, "r");
			
	        byte[] buffer = new byte[chunkSize];
	        logger.info("offset: " + chunkId * Client.chunkSize + " length : " + chunkSize);
	        aFile.seek(chunkId * Client.chunkSize);
	        aFile.readFully(buffer, 0, chunkSize);
//	        int readPos = 0;
//	        while (readPos < buffer.length) {
//	            int nread = aFile.read(buffer, readPos, buffer.length - readPos);
//	            if (nread < 0) {
//	                break;
//	            }
//	            readPos += nread;
//	        }
	        System.out.println("ByteStringData" + ByteString.copyFrom(buffer));
	        chunk = chunk.toBuilder().setData(ByteString.copyFrom(buffer)).build();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Chunk has been updated with file data");
		return chunk;
    }
    
    public static void updateFileWithChunkData(StorageMessages.Chunk chunk) {
    	String fileName = chunk.getFileName();
		int chunkId = chunk.getChunkId();
		int maxChunkNumber = chunk.getMaxChunkNumber();
		File outputFile = new File(Client.fileDestinationPath, fileName);
		
		if(chunkId == 0) {
			// request to controller to get the rest of the chunks
			if(!outputFile.exists() && maxChunkNumber > 1) {
				Client.writeToFile(chunk);
				Client.addChunkToChunkMap(fileName, chunkId, chunk);
				Client.retrieveFileRequestToController(fileName, maxChunkNumber);
			}
		}else {
			Client.writeToFile(chunk);
			Client.addChunkToChunkMap(fileName, chunkId, chunk);
		}
    }
    

    public static void sendMsgWrapperToChannelFutureTemplate(String addr, int port, StorageMessages.MessageWrapper msgWrapper, String initiatedMsg, String successMsg, String failMsg) {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
			MessagePipeline pipeline = new MessagePipeline();
			logger.info(initiatedMsg);
			Bootstrap bootstrap = new Bootstrap()
					.group(workerGroup)
					.channel(NioSocketChannel.class)
					.option(ChannelOption.SO_KEEPALIVE, true)
					.handler(pipeline);

			ChannelFuture cf = bootstrap.connect(addr, port);
			cf.syncUninterruptibly();
			// write msgWrapper to Channel Future
			Channel chan = cf.channel();
			ChannelFuture write = chan.write(msgWrapper);
			chan.flush();
			write.syncUninterruptibly();
			logger.info(successMsg);
			// wait for an object to be updated
			chan.closeFuture().sync();
			workerGroup.shutdownGracefully();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(failMsg);
		}
	}
    
    
    /*
     * This will update the chunk with file byte data
     * This method will save each chunk in seperate thread.
     * Each thread tries to save the chunk in storageNode
     * The first storage node not containing the file becomes the primary node for that chunk
     * If file already exists but not in primary node we do not save it
     */
    public static void saveChunkFromChunkMappings(ChannelHandlerContext ctx, List<StorageMessages.ChunkMapping> chunkMappingList){
    	
    	System.out.println("ChunkMapping count received from controller: " + String.valueOf(chunkMappingList.size()));
    	for (StorageMessages.ChunkMapping chunkMapping : chunkMappingList) {
    		StorageMessages.Chunk chunk = chunkMapping.getChunk();
    		
    		// Update client metadata about current file chunk transfers
    		String chunkKey = chunk.getFileName() + "_" + chunk.getChunkId();
    		if(Client.chunkMapPut.containsKey(chunkKey)) {
    			StorageMessages.Chunk oldChunk = chunkMapPut.get(chunkKey);
    			chunk = chunk.toBuilder()
					.setPrimaryCount(oldChunk.getPrimaryCount())
					.setReplicaCount(oldChunk.getReplicaCount())
					.setReplicaCount(0)
					.build();
    		}else {
    			chunk = chunk.toBuilder()
					.setPrimaryCount(0)
					.setReplicaCount(0)
					.build();
    		}
    		
    		Client.chunkMapPut.put(chunkKey, chunk);
    		chunk = Client.updateChunkWithFileData(chunk);
    		List<StorageMessages.StorageNode> storageNodeList = chunkMapping.getStorageNodeObjsList();

    		System.out.println("Storage Node count received from controller for chunk: " + String.valueOf(chunkMapping.getStorageNodeObjsList().size()));
			for ( int i=0; i< storageNodeList.size(); i++) {
				if(Client.chunkMapPut.get(chunkKey).getPrimaryCount()<1 
						|| Client.chunkMapPut.get(chunkKey).getReplicaCount()<Client.MAX_REPLICAS) {
					StorageMessages.StorageNode storageNode = storageNodeList.get(i);
					boolean isNewChunk = false;
					if (i == storageNodeList.size()-1) {
						isNewChunk = true;
					}
					
					try {
						EventLoopGroup workerGroup = new NioEventLoopGroup();
				        MessagePipeline pipeline = new MessagePipeline();
				        
				        logger.info("Save File Chunk initiated to storageNode: " + storageNode.getStorageNodeAddr() + "/:" + String.valueOf(storageNode.getStorageNodePort()));
				        Bootstrap bootstrap = new Bootstrap()
				            .group(workerGroup)
				            .channel(NioSocketChannel.class)
				            .option(ChannelOption.SO_KEEPALIVE, true)
				            .handler(pipeline);
				        
				        ChannelFuture cf = bootstrap.connect(storageNode.getStorageNodeAddr(), storageNode.getStorageNodePort());
				        cf.syncUninterruptibly();
				
				        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructStoreChunkRequest(chunk, storageNode, true, isNewChunk);
				
				        Channel chan = cf.channel();
				        ChannelFuture write = chan.write(msgWrapper);
				        chan.flush();
				        write.syncUninterruptibly();
				        logger.info("Save File Chunks completed at storageNode: " + storageNode.getStorageNodeId());
				        chan.closeFuture().sync();
				        workerGroup.shutdownGracefully();
					} catch (Exception e) {
						e.printStackTrace();
						logger.error("Save File Chunk failed. Storage node connection establishment failed");
					}
				}
			}
		}
    	ctx.close();
    }
    
    public static void updateChunkSaveStatus(StorageMessages.StoreChunkResponse storeChunkResponse) {
    	StorageMessages.Chunk chunk = storeChunkResponse.getChunk();
    	String fileKey = chunk.getFileName() + "_" + chunk.getChunkId();
    	Client.chunkMapPut.put(fileKey, chunk);
    	logger.info("Updated chunkmap after saving chunk");
    }
    
    
    /*
     * This sends a request to controller to get list of storage nodes
     * to save for each chunk. Opens a channel to controller with 
     * fileName, chunkId, chunksize
     */
    public synchronized void getFile(String fileName) {
		retrieveFileRequestToController(fileName, 1);
    }

    public static void main(String[] args) throws IOException {
        String configFileName;
        String operation; // supported are "GET" or "PUT"
        String absoluteFilePath;
        
        if(args.length>=3) {
            configFileName= args[0];
            operation = args[1];
            absoluteFilePath = args[2];
            
            Config config = new Config(configFileName);
            Client client = new Client();
            client.setVariables(config);
            if(operation.equals("PUT")) {
            	client.sendFile(absoluteFilePath);
            }else if (operation.equals("GET")){
            	File file = new File(absoluteFilePath);
            	String fileName = file.getName();
            	client.getFile(fileName);
            }else if(operation.equals("STATS")) {
            	Client.getActiveNodeList();
            }else {
            	System.out.println("ERROR: Operation not supported");
            }
        }else {
            System.out.println("Incorrect number of arguments passed. Atleast three arguments (config file path, operation, filepath/filename) are required.");
        }
    }
}
