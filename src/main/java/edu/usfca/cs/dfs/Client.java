package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import edu.usfca.cs.dfs.util.CheckSum;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages.MessageWrapper;
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
	private Integer chunkSize;
	private String controllerNodeAddr;
	private Integer controllerNodePort;
	private Integer fileDestinationPath;
	
    public Client(String configFileName) {

    }
    
    /*
     * This sends a request to controller to get list of storage nodes
     * to save for each chunk. Opens a channel to controller with 
     * fileName, chunkId, chunksize
     */
//    public void sendFile(String filePath) {
//    	File file = new File(filePath);
//    	if (!file.exists()) {
//    		System.out.println("File with the given path: " +   filePath +  " does not exists");
//    		return;
//    	}
//    	String fileName = file.getName();
//    	
//    	try {
//    		EventLoopGroup workerGroup = new NioEventLoopGroup();
//            MessagePipeline pipeline = new MessagePipeline();
//            Bootstrap bootstrap = new Bootstrap()
//                    .group(workerGroup)
//                    .channel(NioSocketChannel.class)
//                    .option(ChannelOption.SO_KEEPALIVE, true)
//                    .handler(pipeline);
//	    	RandomAccessFile aFile = new RandomAccessFile(filePath, "r");
//	        FileChannel inChannel = aFile.getChannel();
//	        Integer bufferSize = this.chunkSize;
//	        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
//	        int chunkId = 1;
//	        while(inChannel.read(buffer) > 0){
//	            buffer.flip();
//	            byte[] arr = new byte[buffer.remaining()];
//	            buffer.get(arr);
//	            
//	            MessageWrapper getPrimaryStorageNodeMessage
//	            	= this.constructGetPrimaryStorageNodeRequestMessage(fileName, chunkId, arr.length);
//	            
//	            StorageNode storageNode = this.getPrimaryStorageNode(bootstrap, getPrimaryStorageNodeMessage);
//	            
//	            MessageWrapper saveChunkMessage = 
//	            		this.constructSaveChunkRequestMessage(fileName, chunkId, arr);
//	            this.saveChunkOnPrimary(bootstrap, saveChunkMessage, storageNode);
//	            buffer.clear();
//	            chunkId++;
//	        }
//	        aFile.close();
//	        workerGroup.shutdownGracefully();
//    	}catch (Exception e) {
//        	System.out.println("Caught exception");
//            e.printStackTrace();	
//        }
//    }
    
    
    /*
     * This sends a request to controller to get list of storage nodes
     * to save for each chunk. Opens a channel to controller with 
     * fileName, chunkId, chunksize
     */
    public void getFile(String fileName) {
//    	try {
//    		EventLoopGroup workerGroup = new NioEventLoopGroup();
//            MessagePipeline pipeline = new MessagePipeline();
//            Bootstrap bootstrap = new Bootstrap()
//                    .group(workerGroup)
//                    .channel(NioSocketChannel.class)
//                    .option(ChannelOption.SO_KEEPALIVE, true)
//                    .handler(pipeline);
//    		
//            StorageMessages.RetrieveChunkRequest retrieveFileMsg
//            	= StorageMessages.RetrieveChunkRequest.newBuilder()
//            	.setFileName(fileName)
//            	.build();
//            
//            StorageMessages.MessageWrapper msgWrapper =
//                    StorageMessages.MessageWrapper.newBuilder()
//                        .setRetrieveChunkRequest(retrieveFileMsg)
//                        .build();
//            
//            ChannelFuture cf = bootstrap.connect(this.controllerNodeAddr, this.controllerNodePort);
//            cf.syncUninterruptibly();
//            Channel chan = cf.channel();
//            ChannelFuture write = chan.write(msgWrapper);
//            chan.flush();
//            write.syncUninterruptibly();
//            //TODO: Need to read response from controller
//            //TODO: Need to handle response from controller
//    	}catch (Exception e) {
//        	System.out.println("Caught exception");
//          e.printStackTrace();	
//    	}
    }
    
    /*
     * This will use protobuf message to create the chunk message
     * This will be used by client to send to controller
     *  
     */
    public MessageWrapper constructGetPrimaryStorageNodeRequestMessage(String fileName, int chunkId, int chunksize) {
//    	StorageMessages.PrimaryStorageNodeRequest primaryStorageNodeRequest
//        = StorageMessages.PrimaryStorageNodeRequest.newBuilder()
//            .setFileName(fileName)
//            .setChunkId(chunkId)
//            .setChunkSize(chunksize)
//            .build();
//    	
//    	StorageMessages.MessageWrapper msgWrapper =
//                StorageMessages.MessageWrapper.newBuilder()
//                    .setPrimaryStorageNodeRequest(primaryStorageNodeRequest)
//                    .build();
//    	
//    	return msgWrapper;
    	return null;
    }
    
    
    /*
     * This will use protobuf message to create the chunk message
     * This will be used by client to send to storageNode to save particular chunk
     */
//    public MessageWrapper constructSaveChunkRequestMessage(String fileName, int chunkId, byte[] chunk) {
//    	ByteString data = ByteString.copyFrom(chunk);
//    	
//    	StorageMessages.StoreChunkRequest storeChunkRequestMessage
//        = StorageMessages.StoreChunkRequest.newBuilder()
//            .setFileName(fileName)
//            .setChunkId(chunkId)
//            .setData(data)
//            .build();
//    	
//    	StorageMessages.MessageWrapper msgWrapper =
//                StorageMessages.MessageWrapper.newBuilder()
//                    .setStoreChunkRequest(storeChunkRequestMessage)
//                    .build();
//    	
//    	return msgWrapper;
//    }
    	
    
    
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

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline();


        Bootstrap bootstrap = new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(pipeline);

        ChannelFuture cf = bootstrap.connect("localhost", 7777);
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
                .build();

        StorageMessages.MessageWrapper msgWrapper =
            StorageMessages.MessageWrapper.newBuilder()
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
