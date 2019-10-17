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
    		logger.info("Received Storage Node Registration Request Message on Controller");
            StorageMessages.StorageNode storageNode = msg.getStorageNodeRegisterRequest().getStorageNode();
    		Controller controller = Controller.getInstance();
    		controller.addStorageNode(storageNode);
    		ctx.close();
    	}else if(messageType == 2){
			logger.info("Received Storage Node updated replica state message from controller");
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
    		controller.receiveHeartBeat(storageNodeMsg);
    		ctx.close();
    	}else if(messageType == 4){
    		logger.info("Get Storage Nodes for saving file chunks received on controller");
    		StorageMessages.GetStorageNodesForChunksRequest getStorageNodesForChunksRequest = msg.getGetStorageNodeForChunksRequest();
    		Controller controller = Controller.getInstance();
    		List<StorageMessages.Chunk> chunkList = getStorageNodesForChunksRequest.getChunkListList();
    		logger.info(chunkList);
    		
    		ArrayList<StorageMessages.ChunkMapping> chunkMappingList = controller.getNodesForChunkSave(chunkList);
    		MessageWrapper msgWrapper = HDFSMessagesBuilder.constructGetStorageNodesForChunksResponse(chunkMappingList);
    		logger.info("Controller Nodes response for chunk mapping:");
    		logger.info(msgWrapper.toString());
    		ctx.writeAndFlush(msgWrapper);
    		ctx.close();
    	}else if(messageType == 5){
    		logger.info("Storage Nodes Mappings for saving files chunks received on client");
    		StorageMessages.GetStorageNodesForChunksResponse storageNodesForChunksResponse
    			= msg.getGetStorageNodesForChunksResponse();
    		logger.info(storageNodesForChunksResponse.toString());
    		
    		List<StorageMessages.ChunkMapping> chunkMapping = storageNodesForChunksResponse.getChunkMappingsList();
    		Client.saveChunkFromChunkMappings(ctx, chunkMapping);
    	}else if(messageType == 6){
			logger.info("Storage node receives chunk to be stored from client");
			StorageMessages.StoreChunkRequest storeChunkRequest = msg.getStoreChunkRequest();
			StorageNode storageNode = StorageNode.getInstance();
			StorageMessages.StoreChunkRequest updatedStoreChunkRequest= storageNode.storeChunk(storeChunkRequest);
			MessageWrapper msgWrapper;
			
			if(updatedStoreChunkRequest!=null) {
				msgWrapper = HDFSMessagesBuilder.constructStoreChunkAck(updatedStoreChunkRequest, true);
			}else {
				msgWrapper = HDFSMessagesBuilder.constructStoreChunkAck(storeChunkRequest, false);
			}
			ctx.writeAndFlush(msgWrapper);
			ctx.close();
    	}else if(messageType == 7){
			logger.info("Client receives store chunk ack");
			if(msg.getStoreChunkResponse().getIsSuccess()) {
				Client.updateChunkSaveStatus(msg.getStoreChunkResponse());
			}else {
				throw new Exception("Failed to save chunk on storage Node");
			}
			ctx.close();
    	}else if(messageType == 8){
    		StorageMessages.RetrieveFileChunkMappingRequest retrieveFileChunkMappingRequest = msg.getRetrieveFileChunkMappingRequest();
			logger.info("Retrieve File Request: Controller receives retrieve file request from client to get storage nodes that may contains the file");
			logger.info(retrieveFileChunkMappingRequest.toString());
			
			StorageMessages.Chunk chunk = retrieveFileChunkMappingRequest.getChunk();
			String fileName = chunk.getFileName();
			int maxChunkNumber = chunk.getMaxChunkNumber();
			boolean isZero = retrieveFileChunkMappingRequest.getIsZero();
			
			Controller controller = Controller.getInstance();
			List<StorageMessages.ChunkMapping> chunkMappings = controller.getNodesForRetrieveFile(fileName, maxChunkNumber);
			if(chunkMappings != null) {
				StorageMessages.MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRetrieveFileChunkMappingResponse(chunkMappings, isZero);
				ChannelFuture future = ctx.writeAndFlush(msgWrapper);
				future.addListener(ChannelFutureListener.CLOSE);
			} else {
				logger.error("There are one or more chunk that could not be found on any storage node, stopping retreive");
				throw new Exception("One or more chunks does not even have one replica on storage nodes");
			}
			ctx.close();
		}else if(messageType == 9) {
    		StorageMessages.RetrieveFileChunkMappingResponse retrieveFileResponse = msg.getRetrieveFileChunkMappingResponse();
    		logger.info("Retrieve File Response: Client receives storage nodes mappings from Controller");
    		logger.info(retrieveFileResponse.toString());
    		List<StorageMessages.ChunkMapping> chunkMappings =  retrieveFileResponse.getChunkMappingsList();
    		boolean isZero = retrieveFileResponse.getIsZero();
    		
    		if(chunkMappings.size()== 0) {
    			logger.error("File with this name is not available on any storage nodes");
    			ctx.close();
    		}else {
    			Client.retrieveFile(chunkMappings, isZero);
    			ctx.close();
    		}
		}else if(messageType == 10) {
			logger.info("Retrieve Chunk Request: Storage Node receive request from client");
			StorageMessages.RetrieveChunkRequest retrieveChunkRequest = msg.getRetrieveChunkRequest();
			logger.info(retrieveChunkRequest);
			
			StorageNode storageNode = StorageNode.getInstance();
			StorageMessages.MessageWrapper msgWrapper = storageNode.retrieveChunkFromStorageNode(retrieveChunkRequest);
			ChannelFuture future = ctx.writeAndFlush(msgWrapper);
			future.addListener(ChannelFutureListener.CLOSE);
		}else if(messageType == 11) {
    		logger.info("Retrieve Chunk Response: Client receive chunk from storage node");
    		StorageMessages.RetrieveChunkResponse retrieveChunkResponse = msg.getRetrieveChunkResponse();
    		boolean isZero = retrieveChunkResponse.getIsZero();
    		StorageMessages.Chunk chunkMsg = retrieveChunkResponse.getChunk();

			if(chunkMsg != null) {
				Client.updateFileWithChunkData(chunkMsg, isZero);
			}else {
				logger.error("Chunk doesnt exist on this storage node. False positive detected");
			}
			ctx.close();
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
		}else if (messageType==14) {
			logger.info("Retrieve Chunk to Recover Request: Storage Node receive request from another storage node");
			StorageMessages.RecoverChunkRequest recoverChunkRequest = msg.getRecoverChunkRequest();
			StorageMessages.Chunk chunk = recoverChunkRequest.getChunk();
			String fileName = chunk.getFileName();
			int chunkId = chunk.getChunkId();
			String storageNodeId = recoverChunkRequest.getStorageNodeId();
			// return the chunk to the storage node that request the chunk
			StorageNode storageNode = StorageNode.getInstance();
			// construct retrieve chunk response storage message
			MessageWrapper msgWrapper = storageNode.retrieveRecoverChunk(fileName, chunkId, storageNodeId);
			ChannelFuture future = ctx.writeAndFlush(msgWrapper);
			future.addListener(ChannelFutureListener.CLOSE);
		}else if (messageType==15) {
			logger.info("Retrieve Chunk to Recover Response: Storage receive chunk to be recovered from storage node");
			StorageMessages.RecoverChunkResponse recoverChunkResponse = msg.getRecoverChunkResponse();
			StorageMessages.Chunk chunk = recoverChunkResponse.getChunk();
			String storageNodeId = recoverChunkResponse.getStorageNodeId();
			ctx.close();
			StorageNode storageNode = StorageNode.getInstance();
			storageNode.storeRecoverChunk(chunk, storageNodeId);
		} else if (messageType==16) {
    		logger.info("Controller gets request to return the storageNode object");
			StorageMessages.RecoverChunkResponse recoverChunkResponse = msg.getRecoverChunkResponse();
			StorageMessages.GetActiveStorageNodeListRequest getActiveStorageNodeListRequest = msg.getGetActiveStorageNodeListRequest();
			Controller controller = Controller.getInstance();
			controller.getActiveStorageNodes();
    		// return the requested storageNode object to the storageNode
			StorageMessages.StorageNode storageNodeMsg;
			StorageNode storageNode = StorageNode.getInstance();
//			storageNode.getChunk(addr, port, fileName, chunkNumber, recoverStorageNodeId);

		} else if (messageType==17) {
    		logger.info("StorageNode gets storage node object to recover chunk from controller");


    		// call another storage node in messageType == 15

		}else if(messageType==18) {
			logger.info("Request for getting active node list received on controller");
			Controller controller = Controller.getInstance();
			StorageMessages.MessageWrapper msgWrapper 
				= HDFSMessagesBuilder.constructGetActiveStorageNodeListResponse(controller.getActiveStorageNodes());

			ctx.writeAndFlush(msgWrapper);
			ctx.close();
		}else if(messageType==19) {
			logger.info("Active List of Storage Nodes received from controller!!!");
			List<StorageMessages.StorageNode> activeStorageList = msg.getGetActiveStorageNodeListResponse().getActiveStorageNodesList();
			System.out.println(activeStorageList);
			ctx.close();

		}
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
