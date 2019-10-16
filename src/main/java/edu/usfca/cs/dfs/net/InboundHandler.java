package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

@ChannelHandler.Sharable
public class InboundHandler
extends SimpleChannelInboundHandler<StorageMessages.MessageWrapper> {

	static Logger logger = LogManager.getLogger(Client.class);

    public InboundHandler() { }

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
     * MessageType = 3 for heartbeat request from storageNode
     * MessageType = 6 for getting list of storage nodes for chunk save
     * MessageType = 7 response from controller to client for getting list of storage nodes for chunk save 
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.MessageWrapper msg) throws Exception {
    	int messageType = msg.getMessageType();
    	if(messageType == 1){
			logger.info("Received Storage Node Registration Message");
            StorageMessages.StorageNode storageNode = msg.getStorageNodeRegisterRequest().getStorageNode();
    		Controller controller = Controller.getInstance();
    		storageNode = controller.addStorageNode(storageNode);
    		MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRegisterNodeResponse(storageNode);
    		logger.info("Sending Storage Node Registration Response Message");
    		ChannelFuture future = ctx.writeAndFlush(msgWrapper);
    		future.addListener(ChannelFutureListener.CLOSE);
    	}else if(messageType == 2){
			logger.info("Received Storage Node Registration Response Message");
    		StorageMessages.StorageNodeRegisterResponse storageNodeRegisterResponse = msg.getStorageNodeRegisterResponse();
    		StorageMessages.StorageNode storageNodeMsg = storageNodeRegisterResponse.getStorageNode();
    		StorageNode storageNode = StorageNode.getInstance();
    		logger.info(storageNodeMsg.toString());
    		storageNode.setReplicaStorageNodes(storageNodeMsg.getReplicaNodesList());
    		ctx.close();
    	}else if(messageType == 3){
			logger.info("Heartbeat received on controller");
    		StorageMessages.StorageNodeHeartbeat storageNodeHeartBeat = msg.getStorageNodeHeartBeatRequest().getStorageNodeHeartbeat();
    		StorageMessages.StorageNode storageNodeMsg = storageNodeHeartBeat.getStorageNode();
    		Controller controller = Controller.getInstance();
    		StorageMessages.StorageNode updatedstorageNodeMsg = controller.receiveHeartBeat(storageNodeMsg);
    		ctx.close();
    	}else if(messageType == 4){
			logger.info("Storage node receives chunk to be stored from client");
			StorageMessages.StoreChunkRequest storeChunkRequest = msg.getStoreChunkRequest();
			StorageMessages.Chunk chunk = storeChunkRequest.getChunk();
			StorageNode storageNode = StorageNode.getInstance();
			StorageMessages.StoreChunkRequest updatedStoreChunkRequest= storageNode.storeChunk(storeChunkRequest);
			MessageWrapper msgWrapper;
			
			if(updatedStoreChunkRequest!=null) {
				msgWrapper = HDFSMessagesBuilder.constructStoreChunkAck(updatedStoreChunkRequest, true);
			}else {
				msgWrapper = HDFSMessagesBuilder.constructStoreChunkAck(storeChunkRequest, false);
			}
			ChannelFuture future = ctx.writeAndFlush(msgWrapper);
			future.addListener(ChannelFutureListener.CLOSE);
    	}else if(messageType == 5){
			logger.info("Client receives store chunk ack");
			if(msg.getStoreChunkResponse().getIsSuccess()) {
				Client.updateChunkSaveStatus(msg.getStoreChunkResponse().getChunk());
			}else {
				throw new Exception("Failed to save chunk on storage Node");
			}
			ctx.close();
    	}else if(messageType == 6){
    		logger.info("Get Storage Nodes for saving file chunks received on controller");
    		StorageMessages.GetStorageNodesForChunksRequest getStorageNodesForChunksRequest = msg.getGetStorageNodeForChunksRequest();
    		logger.info(getStorageNodesForChunksRequest);
    		Controller controller = Controller.getInstance();
    		
    		StorageMessages.GetStorageNodesForChunksResponse.Builder responseMsg = StorageMessages.GetStorageNodesForChunksResponse.newBuilder();
    		List<StorageMessages.Chunk> chunkList = getStorageNodesForChunksRequest.getChunkListList();
    		ArrayList<StorageMessages.ChunkMapping> chunkMappingList = controller.getNodesForChunkSave(chunkList);
    		responseMsg.addAllChunkMappings(chunkMappingList);
    		
    		StorageMessages.GetStorageNodesForChunksResponse getStorageNodesForChunksResponse = responseMsg.build();
    		
    		StorageMessages.MessageWrapper msgWrapper =
    		        StorageMessages.MessageWrapper.newBuilder()
    		                .setMessageType(7)
    		                .setGetStorageNodesForChunksResponse(getStorageNodesForChunksResponse)
    		                .build();
    		logger.info("Controller Nodes response for chunk mapping:");
    		logger.info(msgWrapper.toString());
    		ChannelFuture future = ctx.writeAndFlush(msgWrapper);
    		future.addListener(ChannelFutureListener.CLOSE);
    	}else if(messageType == 7){
    		logger.info("Storage Nodes for saving files chunks received on client");
    		StorageMessages.GetStorageNodesForChunksResponse storageNodesForChunksResponse
    			= msg.getGetStorageNodesForChunksResponse();
    		
    		List<StorageMessages.ChunkMapping> chunkMapping = storageNodesForChunksResponse.getChunkMappingsList();
    		Client.saveChunkFromChunkMappings(ctx, chunkMapping);
    	}else if(messageType == 8){
			logger.info("Retrieve File Request: Controller receives retrieve file request from client to get storage nodes that may contains the file ");
			StorageMessages.RetrieveFileRequest retrieveFileRequest = msg.getRetrieveFileRequest();
			StorageMessages.Chunk chunk = retrieveFileRequest.getChunk();
			String fileName = chunk.getFileName();
			int maxChunkNumber = chunk.getMaxChunkNumber();
			Controller controller = Controller.getInstance();
			List<StorageMessages.ChunkMapping> chunkMappings = controller.getNodesForRetrieveFile(fileName, maxChunkNumber);
			if(chunkMappings != null) {
				StorageMessages.MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRetrieveFileResponse(chunkMappings);
				ChannelFuture future = ctx.writeAndFlush(msgWrapper);
				future.addListener(ChannelFutureListener.CLOSE);
			} else {
				logger.info("If there are one or more chunk that could not be found on any storage node, stop");
			}
//			ctx.close();
			// controller send nodes to clients

		}else if(messageType == 9) {
    		logger.info("Retrieve File Response: Client receives storage nodes from Controller");
    		StorageMessages.RetrieveFileResponse retrieveFileResponse = msg.getRetrieveFileResponse();
    		List<StorageMessages.ChunkMapping> chunkMappings =  retrieveFileResponse.getChunkMappingsList();
    		// done getting storage nodes
    		ctx.close();
    		// client retrieve file from storage nodes chunk by chunk
    		Client.retrieveFile(chunkMappings);
		}else if(messageType == 10) {
			logger.info("Retrieve Chunk Request: Storage Node receive request from client");
			StorageMessages.RetrieveChunkRequest retrieveChunkRequest = msg.getRetrieveChunkRequest();
			StorageMessages.Chunk chunk = retrieveChunkRequest.getChunk();
			String fileName = chunk.getFileName();
			int chunkId = chunk.getChunkId();
			// return the chunk to the client
			StorageNode storageNode = StorageNode.getInstance();
			storageNode.retrieveChunk(fileName, chunkId);
			// construct retrieve chunk response storage message
			MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRetrieveChunkResponse(chunk);
			ChannelFuture future = ctx.writeAndFlush(msgWrapper);
			future.addListener(ChannelFutureListener.CLOSE);
		}else if(messageType == 11) {
    		logger.info("Retrieve Chunk Response: Client receive chunk from storage node");
    		StorageMessages.RetrieveChunkResponse retrieveChunkResponse = msg.getRetrieveChunkResponse();
    		//TODO: Update chunk metadata on client
    		//TODO: Required for retries and raise errors
    		StorageMessages.Chunk chunkMsg = retrieveChunkResponse.getChunk();
			String fileName = chunkMsg.getFileName();
			int chunkId = chunkMsg.getChunkId();
    		//store the chunk
			// if chunk is null, it means chunk does not exist
			if(chunkMsg != null) {
				ctx.close();
				if(chunkId == 0) {
					// get maxChunkNumber
					int maxChunkNumber = chunkMsg.getMaxChunkNumber();
					// request to controller to get the rest of the chunks
					Client.retrieveFileRequestToController(fileName, maxChunkNumber);
				} else {
					Client.addChunkToChunkMap(fileName, chunkId, chunkMsg);
					// mock writing to file
					Client.writeToFile(fileName, chunkId, chunkMsg.getData().toByteArray());
				}
			}
			// merge and write it to file later
		}else if(messageType == 12) {
    		logger.info("Save Chunk Update request received on Controller");
    		StorageMessages.StoreChunkControllerUpdateRequest storageChunkControllerUpdateRequest 
    			= msg.getStoreChunkControllerUpdateRequest();
    		logger.info(storageChunkControllerUpdateRequest.toString());
    		StorageMessages.Chunk chunk = storageChunkControllerUpdateRequest.getChunk();
    		StorageMessages.StorageNode storageNode = storageChunkControllerUpdateRequest.getStorageNode();
    		Controller controller = Controller.getInstance();
    		controller.updateChunkSaveonController(storageNode, chunk);
    		logger.info("Save chunk update request handled in controller. Updated metadata");
    		ctx.close();
		}else if(messageType==13) {
			logger.info("HeartBeat Response received on StorageNode");
			StorageMessages.StorageNodeHeartbeat storageNodeHeartBeat = msg.getStorageNodeHeartBeatResponse().getStorageNodeHeartbeat();
			StorageMessages.StorageNode storageNodeMsg = storageNodeHeartBeat.getStorageNode();
			StorageNode storageNode = StorageNode.getInstance();
			storageNode.setReplicaStorageNodes(storageNodeMsg.getReplicaNodesList());
		}
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
