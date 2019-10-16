package edu.usfca.cs.dfs;

import java.io.File;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HDFSMessagesBuilder {
	static Logger logger = LogManager.getLogger(HDFSMessagesBuilder.class);

	private static synchronized long getMaxChunkNumber(long fileSize, int chunkSize) {
    	int maxChunkNumber;
    	maxChunkNumber = (int) fileSize/chunkSize;
    	if(fileSize%chunkSize !=0) {
    		maxChunkNumber++; 
    	}
    	return maxChunkNumber;
    }
	
    /*
     * This will use protobuf message to register storageNode on controller
     * Sets message type as 1 for inbound handler to identify as registration request message
     */
    public static synchronized StorageMessages.MessageWrapper constructRegisterNodeRequest(StorageNode storageNode){
        StorageMessages.StorageNode storageNodeMsg
                = StorageMessages.StorageNode.newBuilder()
                .setStorageNodeId(storageNode.getStorageNodeId())
                .setStorageNodeAddr(storageNode.getStorageNodeAddr())
                .setStorageNodePort(storageNode.getStorageNodePort())
                .setAvailableStorageCapacity(storageNode.getAvailableStorageCapacity())
                .setMaxStorageCapacity(storageNode.getMaxStorageCapacity())
                .build();
        
        StorageMessages.StorageNodeRegisterRequest storageNodeRegisterRequestMsg
        	= StorageMessages.StorageNodeRegisterRequest.newBuilder()
        	.setStorageNode(storageNodeMsg)
        	.build();
        
        StorageMessages.MessageWrapper msgWrapper
                = StorageMessages.MessageWrapper.newBuilder()
                .setMessageType(1)
                .setStorageNodeRegisterRequest(storageNodeRegisterRequestMsg)
                .build();
        return msgWrapper;
    }
    
    /*
     * This will use protobuf message to send response from controller to  storageNode
     * Sets message type as 2 for inbound handler to identify as registration response message
     */
    public static synchronized StorageMessages.MessageWrapper constructRegisterNodeResponse(StorageMessages.StorageNode storageNodeMsg){
		StorageMessages.StorageNodeRegisterResponse storageNodeRegisterResponseMsg
			= StorageMessages.StorageNodeRegisterResponse.newBuilder()
			.setStorageNode(storageNodeMsg)
			.build();
		
		StorageMessages.MessageWrapper msgWrapper
			= StorageMessages.MessageWrapper.newBuilder()
            .setMessageType(2)
            .setStorageNodeRegisterResponse(storageNodeRegisterResponseMsg)
            .build();
		return msgWrapper;
    }
    
    /*
     * This will use protobuf message to send heartbeat from storageNode to controller
     * Sets message type as 3 for inbound handler to identify as heartbeat request message
     */
	public static synchronized StorageMessages.MessageWrapper constructHeartBeatRequest(StorageNode storageNode) {
		StorageMessages.StorageNode storageNodeMsg 
			= StorageMessages.StorageNode.newBuilder()
			.setStorageNodeId(storageNode.getStorageNodeId())
			.setStorageNodeAddr(storageNode.getStorageNodeAddr())
			.setStorageNodePort(storageNode.getStorageNodePort())
			.setAvailableStorageCapacity(storageNode.getAvailableStorageCapacity())
			.setMaxStorageCapacity(storageNode.getMaxStorageCapacity())
			.build();
		
		StorageMessages.StorageNodeHeartbeat StorageNodeHeartBeatMsg
			= StorageMessages.StorageNodeHeartbeat.newBuilder()
			.setStorageNode(storageNodeMsg)
			.build();
		
		StorageMessages.StorageNodeHeartBeatRequest storageNodeHeartBeatRequestMsg
	    	= StorageMessages.StorageNodeHeartBeatRequest.newBuilder()
	    	.setStorageNodeHeartbeat(StorageNodeHeartBeatMsg)
	    	.build();
		
		StorageMessages.MessageWrapper msgWrapper =
		        StorageMessages.MessageWrapper.newBuilder()
		                .setMessageType(3)
		                .setStorageNodeHeartBeatRequest(storageNodeHeartBeatRequestMsg)
		                .build();
		return msgWrapper;
    }
	
	
	/*
     * This will use protobuf message from client to controller requesting storageNodes for file
     * Sets message type as 6 for inbound handler to identify as GetStorageNodesForChunksRequest
     */
	public static synchronized StorageMessages.MessageWrapper constructGetStorageNodesForChunksRequest(File file, int chunkSize) {
		String fileName = file.getName();
		String fileAbsolutePath = file.getAbsolutePath();
    	long fileSize = file.length();
    	long maxChunkNumber = HDFSMessagesBuilder.getMaxChunkNumber(fileSize, chunkSize);
    	
    	StorageMessages.GetStorageNodesForChunksRequest.Builder getStorageNodesForChunksRequestBuilder 
    		= StorageMessages.GetStorageNodesForChunksRequest.newBuilder();
    	
    	long tempfileSize = fileSize;
    	for(int i=0; i<maxChunkNumber; i++) {
    		long tempChunkSize;
    		if(tempfileSize>=chunkSize) {
    			tempChunkSize = chunkSize;
    		}else {
    			tempChunkSize = tempfileSize;
    		}
    		StorageMessages.Chunk chunk = StorageMessages.Chunk.newBuilder()
    			.setChunkId(i)
    			.setFileName(fileName)
    			.setChunkSize((int)tempChunkSize)
    			.setFileAbsolutePath(fileAbsolutePath)
    			.build();
    		
    		getStorageNodesForChunksRequestBuilder.addChunkList(i, chunk);
    		tempfileSize = tempfileSize - chunkSize;
    	}
		
		StorageMessages.MessageWrapper msgWrapper =
		        StorageMessages.MessageWrapper.newBuilder()
		                .setMessageType(6)
		                .setGetStorageNodeForChunksRequest(getStorageNodesForChunksRequestBuilder.build())
		                .build();
		
		logger.info("Message sent for retreiving storage nodes for chunk");
		logger.info(msgWrapper.toString());
		return msgWrapper;
    }
	
	public static synchronized StorageMessages.ChunkMapping constructChunkMapping (
		StorageMessages.Chunk chunk, ArrayList<StorageMessages.StorageNode> storageNodeList) {
		
		StorageMessages.ChunkMapping.Builder chunkMappingMsg  = StorageMessages.ChunkMapping.newBuilder();
		chunkMappingMsg.setChunk(chunk);
		
		chunkMappingMsg.addAllStorageNodeObjs(storageNodeList);
		StorageMessages.ChunkMapping chunkMapping = chunkMappingMsg.build();
		
		return chunkMapping;
	}

    public static synchronized StorageMessages.MessageWrapper constructStoreChunkRequest(
    		StorageMessages.Chunk chunk, StorageMessages.StorageNode storageNode, boolean isClientInitated, boolean isNewChunk) {
        StorageMessages.StoreChunkRequest storeChunkRequest = StorageMessages.StoreChunkRequest.newBuilder()
                .setChunk(chunk)
                .setStorageNode(storageNode)
                .setIsClientInitiated(isClientInitated)
                .setIsNewChunk(isNewChunk)
                .build();
        StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
                .setMessageType(4)
                .setStoreChunkRequest(storeChunkRequest)
                .build();
        return msgWrapper;
    }

    public static synchronized StorageMessages.MessageWrapper constructStoreChunkAck(StorageMessages.Chunk chunk, boolean isSuccess) {
    	StorageMessages.Chunk chunkMsg 
    		= StorageMessages.Chunk.newBuilder()
    			.setChunkId(chunk.getChunkId())
    			.setChunkSize(chunk.getChunkSize())
    			.setFileName(chunk.getFileName())
    			.setChecksum(chunk.getChecksum())
    			.setMaxChunkNumber(chunk.getMaxChunkNumber())
    			.setFileAbsolutePath(chunk.getFileAbsolutePath())
    			.setPrimaryCount(chunk.getPrimaryCount())
    			.setReplicaCount(chunk.getReplicaCount())
    			.build();
    	
        StorageMessages.StoreChunkResponse storeChunkResponse = StorageMessages.StoreChunkResponse.newBuilder()
                .setChunk(chunkMsg)
                .setIsSuccess(isSuccess)
                .build();
        StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
                .setMessageType(5)
                .setStoreChunkResponse(storeChunkResponse)
                .build();
	    return msgWrapper;
    }
    
    public static synchronized StorageMessages.MessageWrapper constructStoreChunkControllerUpdateRequest(StorageMessages.Chunk chunk, StorageNode storageNode) {
    	StorageMessages.Chunk chunkMsg 
    		= StorageMessages.Chunk.newBuilder()
    			.setChunkId(chunk.getChunkId())
    			.setFileName(chunk.getFileName())
    			.build();
    	
    	StorageMessages.StorageNode storageNodeMsg 
			= StorageMessages.StorageNode.newBuilder()
			.setStorageNodeId(storageNode.getStorageNodeId())
			.setStorageNodeAddr(storageNode.getStorageNodeAddr())
			.setStorageNodePort(storageNode.getStorageNodePort())
			.setAvailableStorageCapacity(storageNode.getAvailableStorageCapacity())
			.setMaxStorageCapacity(storageNode.getMaxStorageCapacity())
			.build();
    	
        StorageMessages.StoreChunkControllerUpdateRequest storeChunkControllerUpdateRequestMsg 
        	= StorageMessages.StoreChunkControllerUpdateRequest.newBuilder()
                .setChunk(chunkMsg)
                .setStorageNode(storageNodeMsg)
                .build();
        
        StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
                .setMessageType(12)
                .setStoreChunkControllerUpdateRequest(storeChunkControllerUpdateRequestMsg)
                .build();
	    return msgWrapper;
    }
    

    public static synchronized StorageMessages.MessageWrapper constructRetrieveFileRequest(String fileName, int chunkId) {
		StorageMessages.Chunk chunkMsg = StorageMessages.Chunk.newBuilder()
				.setFileName(fileName)
				.setChunkId(chunkId)
				.build();
		StorageMessages.RetrieveFileRequest retrieveFileRequest = StorageMessages.RetrieveFileRequest.newBuilder()
				.setChunk(chunkMsg)
				.build();
		StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
				.setMessageType(8)
				.setRetrieveFileRequest(retrieveFileRequest)
				.build();
        return msgWrapper;
    }

    public static synchronized StorageMessages.MessageWrapper constructRetrieveFileResponse(StorageMessages.ChunkMapping chunkMapping) {
		StorageMessages.RetrieveFileResponse retrieveFileResponse = StorageMessages.RetrieveFileResponse.newBuilder()
				.setChunkMappings(chunkMapping)
				.build();
		StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
				.setMessageType(9)
				.setRetrieveFileResponse(retrieveFileResponse)
				.build();
		return msgWrapper;
	}

    public static synchronized StorageMessages.MessageWrapper constructRetrieveChunkRequest(String fileName, int chunkId) {
		StorageMessages.Chunk chunkMsg = StorageMessages.Chunk.newBuilder()
				.setFileName(fileName)
				.setChunkId(chunkId)
				.build();
		StorageMessages.RetrieveChunkRequest retrieveChunkRequest = StorageMessages.RetrieveChunkRequest.newBuilder()
				.setChunk(chunkMsg)
				.build();
		StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
				.setMessageType(8)
				.setRetrieveChunkRequest(retrieveChunkRequest)
				.build();
		return msgWrapper;
    }

	public static synchronized StorageMessages.MessageWrapper constructRetrieveChunkResponse(StorageMessages.Chunk chunk) {
		StorageMessages.RetrieveChunkResponse retrieveChunkResponse = StorageMessages.RetrieveChunkResponse.newBuilder()
				.setChunk(chunk)
				.build();

		StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
				.setMessageType(10)
				.setRetrieveChunkResponse(retrieveChunkResponse)
				.build();
		return msgWrapper;
	}

    public static synchronized StorageMessages.MessageWrapper constructRetrieveChunkAck() {
        return null;
    }
}
