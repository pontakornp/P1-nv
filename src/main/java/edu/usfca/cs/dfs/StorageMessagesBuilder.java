package edu.usfca.cs.dfs;

public class StorageMessagesBuilder {

    public static StorageMessages.StorageNodeRegisterRequest getNodesForChunkStoreRequest() {
        return null;
    }

    /*
     * This will use protobuf message to create the chunk message
     * This will be used by client to send to storageNode to save particular chunk
     */
    public static StorageMessages.MessageWrapper constructGetPrimaryNodeRequest
            (
                String storageNodeId,
                String storageNodeAddr,
                int storageNodePort,
                int currentStorageValue,
                int maxStorageValue
             ) {
        StorageMessages.StorageNode storageNode
                = StorageMessages.StorageNode.newBuilder()
                .setStorageNodeId(storageNodeId)
                .setStorageNodeAddr(storageNodeAddr)
                .setStorageNodePort(storageNodePort)
                .setCurrentStorageValue(currentStorageValue)
                .setMaxStorageValue(maxStorageValue)
                .build();

        StorageMessages.StorageNodeRegisterRequest registerNodeMessage
                = StorageMessages.StorageNodeRegisterRequest.newBuilder()
                .setStorageNode(storageNode)
                .build();

        StorageMessages.MessageWrapper msgWrapper =
                StorageMessages.MessageWrapper.newBuilder()
                        .setMessageType(1)
                        .setStorageNodeRegisterRequest(registerNodeMessage)
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
