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
		logger.info("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
		logger.info("Connection lost: " + addr);
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
			logger.info("Received Storage Node Registration Message");
            StorageMessages.StorageNode storageNode = msg.getStorageNodeRegisterRequest().getStorageNode();
    		Controller controller = Controller.getInstance();
    		controller.addStorageNode(storageNode);
    		
    		MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRegisterNodeResponse(storageNode);
    		logger.info("Sending Storage Node Registration Response Message");
    		ChannelFuture future = ctx.writeAndFlush(msgWrapper);
    		ctx.close();
    	}else if(messageType == 2){
			logger.info("Received Storage Node Registration Response Message");
    		StorageMessages.StorageNodeRegisterResponse storageNodeRegisterResponse = msg.getStorageNodeRegisterResponse();
    		StorageMessages.StorageNode storageNodeMsg = storageNodeRegisterResponse.getStorageNode();
    		StorageNode storageNode = StorageNode.getInstance();
    		storageNode.setReplicationNodeIds((List<String>) storageNodeMsg.getReplicationNodeIdsList());
    	}else if(messageType == 3){
			logger.info("Heartbeat received on controller");
    		StorageMessages.StorageNodeHeartbeat storageNodeHeartbeat = msg.getStorageNodeHeartBeatRequest().getStorageNodeHeartbeat();
    		String storageNodeId = storageNodeHeartbeat.getStorageNodeId();
    		Controller controller = Controller.getInstance();
    		controller.receiveHeartBeat(storageNodeId);
			logger.info("Heartbeat updated on controller for storageNodeId: " + storageNodeId);
    		ctx.close();
    	}else if(messageType == 4){
			logger.info("Storage node receives chunk to be stored from client");
			StorageMessages.StoreChunkRequest storeChunkRequest = msg.getStoreChunkRequest();
			StorageMessages.Chunk chunk = storeChunkRequest.getChunk();
			StorageNode storageNode = StorageNode.getInstance();
			boolean isSuccess = storageNode.storeChunk(chunk.getFileName(), chunk.getChunkId(), chunk.getData().toByteArray(), chunk.getChecksum());
			boolean isPrimary = storeChunkRequest.getIsPrimary();
			// if its a primary node, then the chunk will be replicated to 2 other nodes
			if (isPrimary) {
				boolean isReplicated = storageNode.storeChunkOnReplica(chunk);
				if (!isReplicated) {
					// retry
				}
			}
			// send ack response if chunk is stored successfully in primary nodes and its replicas
			MessageWrapper msgWrapper = HDFSMessagesBuilder.constructStoreChunkAck(chunk, isSuccess);
			ChannelFuture future = ctx.writeAndFlush(msgWrapper);
			future.addListener(ChannelFutureListener.CLOSE);
    	}else if(messageType == 5){
			logger.info("Client receives store chunk ack");

    	}else if(messageType == 6){
    		logger.info("Save File Chunks received on controller");
    		StorageMessages.GetStorageNodesForChunksRequest getStorageNodesForChunksRequest = msg.getGetStorageNodeForChunksRequest();
    		Controller controller = Controller.getInstance();
    		
    		StorageMessages.GetStorageNodesForChunksResponse.Builder responseMsg = StorageMessages.GetStorageNodesForChunksResponse.newBuilder();
    		List<StorageMessages.Chunk> chunkList = getStorageNodesForChunksRequest.getChunkListList();
    		
    		for (int i=0; i< chunkList.size(); i++) {
    			StorageMessages.ChunkMapping chunkMapping = controller.getNodesForChunkSave(chunkList.get(i));
    			responseMsg.setChunkMappings(i, chunkMapping);
    		}
    		
    		StorageMessages.GetStorageNodesForChunksResponse getStorageNodesForChunksResponse = responseMsg.build();
    		
    		StorageMessages.MessageWrapper msgWrapper =
    		        StorageMessages.MessageWrapper.newBuilder()
    		                .setMessageType(7)
    		                .setGetStorageNodesForChunksResponse(getStorageNodesForChunksResponse)
    		                .build();
    		
    		ChannelFuture future = ctx.writeAndFlush(msgWrapper);
    		ctx.close();
    	}else if(messageType == 7){
    		logger.info("File Existence check response received on client");
    		//TODO: Client need to handle the response
    	}
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
