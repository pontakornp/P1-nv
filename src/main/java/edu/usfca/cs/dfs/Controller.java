package edu.usfca.cs.dfs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class Controller {
	private ConcurrentHashMap<String, StorageNode> activeStorageNodes;
	
	/*
	 * This is called during registration of StorageNode
	 * This will add the node metadata to activeStorageNodes
	 * Takes the Storage 
	 */
	public synchronized void  addStorageNode(StorageNode storageNode) {
		
	}
	
	/*
	 * This is called when controller deletes StorageNode when it decides it on inactive status
	 * This will remove the node from activeStorageNodes
	 */
	public synchronized void deleteStorageNode(StorageNode storageNode) {
		
	}
	
	/*
	 *  This will be called by client to get the list of nodes to save a chunk
	 *  This will randomly select three nodes from activeStorageNodes
	 */
	public synchronized CopyOnWriteArraySet<StorageNode> sendNodesForChunkSave() {
		return null;
	}
	
	/*
	 * This will be called when StorageNode send HeartBeat to controller
	 * This will need to update metadata of chunk on controller
	 */
	public synchronized void receiveHeartBeat(HeartBeat heartbeat) {
		
	}
	
	/*
	 * This will be called as response to HeartBeat send from StorageNode
	 */
	public synchronized void sendHeartBeatConfirmation() {
		
	}
	
	/*
	 * This will be called repeatedly and identify the list of inactive nodes
	 * for each inactive node it handles the inactive node by replicating the data on inactive node
	 */
	public synchronized void handleInactiveNodes() {
		
	}
	
	
	/*
	 * This will be called to detect list of inactive nodes
	 */
	public synchronized void detectInactiveNodes() {
		
	}
	
	/*
	 * This will handle the replication of inactive node data and replicate it to another node
	 * This needs to identify the list of chunks on the inactive node
	 * for each chunk on inactive node it replicates the chunk to a new node
	 * 
	 */
	public synchronized void handleInactiveNode() {
		
	}

}
