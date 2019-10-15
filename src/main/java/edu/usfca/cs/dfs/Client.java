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
	private int chunkSize; // This is chunk size in bytes
	private String controllerNodeAddr;
	private Integer controllerNodePort;
	private String fileDestinationPath;
	private static ConcurrentHashMap<String, StorageMessages.Chunk> chunkMapPut;
	private static ConcurrentHashMap<String, StorageMessages.Chunk> chunkMapGet;
	
    public Client() {

    }
    
    private void setVariables(Config config) {
		this.chunkSize = config.getChunkSize();
		this.controllerNodeAddr = config.getControllerNodeAddr();
		this.controllerNodePort = config.getControllerNodePort();
		this.fileDestinationPath = config.getClientDirectoryPath();
		System.out.println("Client Node config updated.");
	}
    
    /*
     * This sends a request to controller to get list of storage nodes
     * to save for each chunk. Opens a channel to controller with 
     * fileName, chunkId, chunksize
     */
    public void sendFile(String filePath) {
    	File file = new File(filePath);
    	if (!file.exists()) {
    		System.out.println("File with the given path: " +   filePath +  " does not exists");
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
	        
	        logger.info("Save File initiated to controller: " + this.controllerNodeAddr + String.valueOf(this.controllerNodePort));
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
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


    public void retrieveFile(String fileName) {
        try {
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            MessagePipeline pipeline = new MessagePipeline();

            logger.info("Retrieve File initiated to controller: " + this.controllerNodeAddr + String.valueOf(this.controllerNodePort));
            Bootstrap bootstrap = new Bootstrap()
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(pipeline);

            ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
            cf.syncUninterruptibly();

            MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRetrieveFileRequest(fileName);

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
	        aFile.seek(chunkId*chunkSize);
	        int readPos = 0;
	        while (readPos < buffer.length) {
	            int nread = aFile.read(buffer, readPos, buffer.length - readPos);
	            if (nread < 0) {
	                break;
	            }
	            readPos += nread;
	        }
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
    		
    		chunk = Client.updateChunkWithFileData(chunk);
    		
    		
    		
    		System.out.println("Storage Node count received from controller for chunk: " + String.valueOf(chunkMapping.getStorageNodeObjsList().size()));
			for (StorageMessages.StorageNode storageNode : chunkMapping.getStorageNodeObjsList()) {
				
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
			
			        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructStoreChunkRequest(chunk, true);
			
			        Channel chan = cf.channel();
			        ChannelFuture write = chan.write(msgWrapper);
			        chan.flush();
			        write.syncUninterruptibly();
			        logger.info("Save File Chunks completed at storageNode");
			        chan.closeFuture().sync();
			        workerGroup.shutdownGracefully();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error("Save File Chunk failed. Storage node connection establishment failed");
				}
			}
		}
    	ctx.close();
    }
    
    
    /*
     * This sends a request to controller to get list of storage nodes
     * to save for each chunk. Opens a channel to controller with 
     * fileName, chunkId, chunksize
     */
    public void getFile(String fileName) {
    }
    
    /*
     * This sends a request to controller to get list of storage nodes
     * to save for each chunk. Opens a channel to controller with 
     * fileName, chunkId, chunksize
     */
    private StorageNode getPrimaryStorageNode(Bootstrap bootstrap, MessageWrapper message) {
        ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
        cf.syncUninterruptibly();
        Channel chan = cf.channel();
        ChannelFuture write = chan.write(message);
        chan.flush();
        write.syncUninterruptibly();
        // TODO: Need to create a two way communication between controller and client
        return null;
    }
    
    private void saveChunkOnPrimary(Bootstrap bootstrap, MessageWrapper message, StorageNode storageNode) {
    	ChannelFuture cf = bootstrap.connect(storageNode.getStorageNodeAddr(), storageNode.getStorageNodePort());
        cf.syncUninterruptibly();
        Channel chan = cf.channel();
        ChannelFuture write = chan.write(message);
        chan.flush();
        write.syncUninterruptibly();
        // TODO: Need to create a two way communication between client and storageNode
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
            }else {
            	System.out.println("ERROR: Operation not supported");
            }
        }else {
            System.out.println("Incorrect number of arguments passed. Atleast three arguments (config file path, operation, filepath/filename) are required.");
        }
    }
}
