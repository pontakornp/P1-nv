package edu.usfca.cs.dfs;

public class HDFSMessagesBuilder {

    /*
     * This will use protobuf message to register storagenode on controller
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
        
        StorageMessages.MessageWrapper msgWrapper =
                StorageMessages.MessageWrapper.newBuilder()
                        .setMessageType(1)
                        .setStorageNodeRegisterRequest(storageNodeRegisterRequestMsg)
                        .build();
        return msgWrapper;
    }
    
    /*
     * This will use protobuf message to register storagenode on controller
     */
    public static StorageMessages.MessageWrapper constructRegisterNodeResponse(StorageMessages.StorageNode storageNodeMsg){
		StorageMessages.StorageNodeRegisterResponse storageNodeRegisterResponseMsg
			= StorageMessages.StorageNodeRegisterResponse.newBuilder()
			.setStorageNode(storageNodeMsg)
			.build();
		
		StorageMessages.MessageWrapper msgWrapper =
		        StorageMessages.MessageWrapper.newBuilder()
		                .setMessageType(2)
		                .setStorageNodeRegisterResponse(storageNodeRegisterResponseMsg)
		                .build();
		return msgWrapper;
    }
    
    
	public static StorageMessages.MessageWrapper constructHeartBeatRequest(StorageNode storageNode) {
		return null;
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
