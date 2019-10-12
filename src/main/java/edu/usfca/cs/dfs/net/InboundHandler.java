package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.List;

import edu.usfca.cs.dfs.Client;
import edu.usfca.cs.dfs.Controller;
import edu.usfca.cs.dfs.HDFSMessagesBuilder;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.MessageWrapper;
import edu.usfca.cs.dfs.StorageNode;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ChannelHandler.Sharable
public class InboundHandler
extends SimpleChannelInboundHandler<StorageMessages.MessageWrapper> {

	static Logger logger = LogManager.getLogger(Client.class);

    public InboundHandler(

	) { }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    /*
     * MessageType = 1 for register initiation request
     * MessageType = 2 for register acknowledge response
     *  
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.MessageWrapper msg) {
    	int messageType = msg.getMessageType();
    	if (messageType == 1) {
    		System.out.println("Received Storage Node Registration Message");
            StorageMessages.StorageNode storageNode = msg.getStorageNodeRegisterRequest().getStorageNode();
    		Controller controller = Controller.getInstance();
    		controller.addStorageNode(storageNode);
    		
    		MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRegisterNodeResponse(storageNode);
        	System.out.println("Sending Storage Node Registration Response Message");
    		ChannelFuture future = ctx.writeAndFlush(msgWrapper);
    		future.addListener(ChannelFutureListener.CLOSE);
    	}else if(messageType == 2){
    		System.out.println("Received Storage Node Registration Response Message");
    		StorageMessages.StorageNodeRegisterResponse storageNodeRegisterResponse = msg.getStorageNodeRegisterResponse();
    		StorageMessages.StorageNode storageNodeMsg = storageNodeRegisterResponse.getStorageNode();
    		StorageNode storageNode = StorageNode.getInstance();
    		storageNode.setReplicationNodeIds((List<String>) storageNodeMsg.getReplicationNodeIdsList());
    	}else if(messageType == 3){
    		System.out.println("Heartbeat received on controller");
    		StorageMessages.StorageNodeHeartbeat storageNodeHeartbeat = msg.getStorageNodeHeartBeatRequest().getStorageNodeHeartbeat();
    		String storageNodeId = storageNodeHeartbeat.getStorageNodeId();
    		Controller controller = Controller.getInstance();
    		controller.receiveHeartBeat(storageNodeId);
    		System.out.println("Heartbeat updated on controller for storageNodeId: " + storageNodeId);
    		ctx.close();
    	}else if(messageType == 4){
			System.out.println("Storage node receives chunk to be stored from client");
			StorageMessages.Chunk chunk = msg.getStoreChunkRequest().getChunk();
			StorageNode storageNode = StorageNode.getInstance();
			boolean isSuccess = storageNode.storeChunk(chunk.getFileName(), chunk.getChunkId(), chunk.getData().toByteArray(), chunk.getChecksum());
			// Send Ack response
			MessageWrapper msgWrapper = HDFSMessagesBuilder.constructStoreChunkAck(chunk, isSuccess);
			ChannelFuture future = ctx.writeAndFlush(msgWrapper);
			future.addListener(ChannelFutureListener.CLOSE);
    	}else if(messageType == 5){
			System.out.println("Client receives store chunk ack");


    	}else if(messageType == 6){

    	}
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
