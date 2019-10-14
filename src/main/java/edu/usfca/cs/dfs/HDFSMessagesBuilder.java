package edu.usfca.cs.dfs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class HDFSMessagesBuilder {

	private static long getMaxChunkNumber(long fileSize, int chunkSize) {
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
    public static StorageMessages.MessageWrapper constructRegisterNodeRequest(StorageNode storageNode){
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
    public static StorageMessages.MessageWrapper constructRegisterNodeResponse(StorageMessages.StorageNode storageNodeMsg){
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
	public static StorageMessages.MessageWrapper constructHeartBeatRequest(StorageNode storageNode) {
		StorageMessages.StorageNode storageNodeMsg 
			= StorageMessages.StorageNode.newBuilder()
			.setStorageNodeId(storageNode.getStorageNodeId())
			.setStorageNodeAddr(storageNode.getStorageNodeAddr())
			.setStorageNodePort(storageNode.getStorageNodePort())
			.setAvailableStorageCapacity(storageNode.getAvailableStorageCapacity())
			.setMaxStorageCapacity(storageNode.getMaxStorageCapacity())
			.addAllReplicaNodes(storageNode.getReplicaStorageNodes())
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
	public static StorageMessages.MessageWrapper constructGetStorageNodesForChunksRequest(File file, int chunkSize) {
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
		return msgWrapper;
    }
	
	public static StorageMessages.ChunkMapping constructChunkMapping (
		StorageMessages.Chunk chunk, ArrayList<StorageMessages.StorageNode> storageNodeList) {
		
		StorageMessages.ChunkMapping.Builder chunkMappingMsg  = StorageMessages.ChunkMapping.newBuilder();
		chunkMappingMsg.setChunk(chunk);
		
		chunkMappingMsg.addAllStorageNodeObjs(storageNodeList);
		StorageMessages.ChunkMapping chunkMapping = chunkMappingMsg.build();
		
		return chunkMapping;
	}

    public static StorageMessages.MessageWrapper constructGetPrimaryNodeResponse() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructGetPrimaryNodesAck() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructStoreChunkRequest(StorageMessages.Chunk chunk, boolean isPrimary) {
        StorageMessages.StoreChunkRequest storeChunkRequest = StorageMessages.StoreChunkRequest.newBuilder()
                .setChunk(chunk)
                .setIsPrimary(isPrimary)
                .build();
        StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
                .setMessageType(4)
                .setStoreChunkRequest(storeChunkRequest)
                .build();
        return msgWrapper;
    }

    public static StorageMessages.MessageWrapper constructStoreChunkAck(StorageMessages.Chunk chunk, boolean isSuccess) {
        StorageMessages.StoreChunkResponse storeChunkResponse = StorageMessages.StoreChunkResponse.newBuilder()
                .setChunk(chunk)
                .setIsSuccess(isSuccess)
                .build();
        StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
                .setMessageType(5)
                .setStoreChunkResponse(storeChunkResponse)
                .build();
	    return msgWrapper;
    }

    public static StorageMessages.MessageWrapper constructAllStorageNodeRequest() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructAllStorageNodeResponse() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructAllStorageNodeAck() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructRetrieveFileRequest(String fileName) {
		StorageMessages.RetrieveFileRequest retrieveFileRequest = StorageMessages.RetrieveFileRequest.newBuilder()
				.setFileName(fileName)
				.build();
		StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
				.setMessageType(8)
				.setRetrieveFileRequest(retrieveFileRequest)
				.build();
        return null;
    }

    public static StorageMessages.MessageWrapper constructRetrieveFileResponse(StorageMessages.ChunkMapping chunkMapping) {
		StorageMessages.RetrieveFileResponse retrieveFileResponse = StorageMessages.RetrieveFileResponse.newBuilder()
				.setChunkMappings(chunkMapping)
				.build();
		StorageMessages.MessageWrapper msgWrapper = StorageMessages.MessageWrapper.newBuilder()
				.setRetrieveFileResponse(retrieveFileResponse)
				.build();
		return msgWrapper;
	}

    public static StorageMessages.MessageWrapper constructRetrieveChunkRequest() {
        return null;
    }

	public static StorageMessages.MessageWrapper constructRetrieveChunkResponse() {
		return null;
	}

    public static StorageMessages.MessageWrapper constructRetrieveChunkAck() {
        return null;
    }
}
