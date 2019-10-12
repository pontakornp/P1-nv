package edu.usfca.cs.dfs;

public class HDFSMessagesBuilder {

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
		StorageMessages.StorageNodeHeartbeat StorageNodeHeartbeatMsg
			= StorageMessages.StorageNodeHeartbeat.newBuilder()
			.setStorageNodeId(storageNode.getStorageNodeId())
			.build();
		
		StorageMessages.StorageNodeHeartBeatRequest storageNodeHeartBeatRequestMsg
	    	= StorageMessages.StorageNodeHeartBeatRequest.newBuilder()
	    	.setStorageNodeHeartbeat(StorageNodeHeartbeatMsg)
	    	.build();
		
		StorageMessages.MessageWrapper msgWrapper =
		        StorageMessages.MessageWrapper.newBuilder()
		                .setMessageType(3)
		                .setStorageNodeHeartBeatRequest(storageNodeHeartBeatRequestMsg)
		                .build();
		return msgWrapper;
    }

    public static StorageMessages.MessageWrapper constructGetPrimaryNodeResponse() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructGetPrimaryNodesAck() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructStoreChunkRequest() {
        return null;
    }

    public static StorageMessages.MessageWrapper constructStoreChunkAck() {
        return null;
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
