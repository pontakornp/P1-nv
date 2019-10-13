package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.util.CheckSum;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.StorageMessages.MessageWrapper;
import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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

    public static void main(String[] args)
    throws IOException {

//
//        String configFileName;
//        if(args.length>0) {
//            configFileName= args[0];
//        }else {
//            configFileName = "config.json";
//        }
//        Config config = new Config(configFileName);

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline();


        Bootstrap bootstrap = new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(pipeline);

        ChannelFuture cf = bootstrap.connect("localhost", 7001);
        cf.syncUninterruptibly();

        ByteString data = ByteString.copyFromUtf8("Hello World!");
        String checkSum = CheckSum.checkSum(data.toByteArray());
        int chunkSize = data.toByteArray().length;
        StorageMessages.Chunk chunk = StorageMessages.Chunk.newBuilder()
                .setFileName("my_file")
                .setChecksum(checkSum)
                .setChunkId(1)
                .setData(data)
                .setChunkSize(chunkSize)
                .build();

        StorageMessages.StoreChunkRequest storeChunkMsg
            = StorageMessages.StoreChunkRequest.newBuilder()
                .setChunk(chunk)
                .setIsPrimary(true)
                .build();

        StorageMessages.MessageWrapper msgWrapper =
            StorageMessages.MessageWrapper.newBuilder()
                .setMessageType(4)
                .setStoreChunkRequest(storeChunkMsg)
                .build();

        Channel chan = cf.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.syncUninterruptibly();

        /* Don't quit until we've disconnected: */
        System.out.println("Shutting down");
        workerGroup.shutdownGracefully();
    }
}
