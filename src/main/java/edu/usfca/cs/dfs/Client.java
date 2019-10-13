package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;

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
	private static HashMap<String, StorageMessages.Chunk> chunkMap;
	
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
    
    public static void updateChunkWithFileData(StorageMessages.Chunk chunk) {
    	String filePath = chunk.getFileAbsolutePath();
    	int chunkId = chunk.getChunkId();
    	int chunkSize = chunk.getChunkSize();
    	
    	RandomAccessFile aFile;
		try {
			aFile = new RandomAccessFile(filePath, "r");
			aFile.seek(chunkId*chunkSize);
			FileChannel inChannel = aFile.getChannel();
			
	        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
	        while(inChannel.read(buffer) > 0){
	            buffer.flip();
	            byte[] arr = new byte[buffer.remaining()];
	            buffer.get(arr);
	            chunk.newBuilder().setData(ByteString.copyFrom(arr));
	            buffer.clear();
	        }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			
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
    	for (StorageMessages.ChunkMapping chunkMapping : chunkMappingList) {
    		StorageMessages.Chunk chunk = chunkMapping.getChunk();
    		
    		Client.updateChunkWithFileData(chunk);

			for (StorageMessages.StorageNode storageNode : chunkMapping.getStorageNodeObjsList()) {
				try {
					EventLoopGroup workerGroup = new NioEventLoopGroup();
			        MessagePipeline pipeline = new MessagePipeline();
			        
			        logger.info("Save File Chunk initiated to storageNode: " + storageNode.getStorageNodeAddr() + String.valueOf(storageNode.getStorageNodePort()));
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
