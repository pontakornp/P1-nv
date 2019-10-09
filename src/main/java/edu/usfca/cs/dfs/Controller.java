package edu.usfca.cs.dfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import edu.usfca.cs.dfs.config.ControllerNodeConfig;

public class Controller {
	private ConcurrentHashMap<String, StorageNode> activeStorageNodes;
	private ConcurrentHashMap<String, BloomFilter> bloomFilterList;
	private static Integer BLOOM_FILTER_SIZE = 1024;
	private static Integer BLOOM_HASH_COUNT = 3;
	
	public Controller(ControllerNodeConfig controllerConfig) {
		
	}
	
	/*
	 * This is called during registration of StorageNode
	 * This will add the node metadata to activeStorageNodes
	 * Takes the Storage 
	 */
	public synchronized void  addStorageNode(StorageNode storageNode) {
		String storageNodeId = storageNode.getStorageNodeId();
		storageNode = this.getReplicationNodes(storageNode);
		if (!this.activeStorageNodes.containsKey(storageNodeId)) {
			activeStorageNodes.put(storageNodeId, storageNode);
			bloomFilterList.put(storageNodeId, new BloomFilter(Controller.BLOOM_FILTER_SIZE, Controller.BLOOM_HASH_COUNT));
		}
	}
	
	/* This will update the replication nodes for storage nodes 
	 * This will randomly select two other registered nodes as replicas for current node
	 */
	private StorageNode getReplicationNodes(StorageNode storageNode) {
		ArrayList<String> duplicateNodeList = new ArrayList<String>(); 
		int count = 0;
		for (Map.Entry<String, StorageNode> existingStorageNode : this.activeStorageNodes.entrySet()) {
			if(existingStorageNode.getKey() != storageNode.getStorageNodeId()){
				duplicateNodeList.add(existingStorageNode.getKey());
				count++;
			}
			if(count==2) {
				break;
			}
		}
		storageNode.setReplicationNodeIds(duplicateNodeList);
		return storageNode;
	}
	
	/*
	 * This is called when controller deletes StorageNode when it decides it on inactive status
	 * This will remove the node from activeStorageNodes
	 */
	public synchronized void deleteStorageNode(StorageNode storageNode){
		this.activeStorageNodes.remove(storageNode.getStorageNodeId());
	}
	
	/*
	 *  This will be called by client to get the list of nodes to save a chunk
	 *  This will randomly select three nodes from activeStorageNodes
	 */
	public synchronized ArrayList<StorageNode>getNodesForChunkSave() {
		ArrayList<StorageNode> nodeList = new ArrayList<StorageNode>();
		List<String> keysAsArray = new ArrayList<String>(this.activeStorageNodes.keySet());
		for (int i = 0; i < 3; i++) {
			Random r = new Random();
			nodeList.add(this.activeStorageNodes.get(keysAsArray.get(r.nextInt(keysAsArray.size()))));
		}
		return nodeList;
	}
	
	/*
	 * This will be called when StorageNode send HeartBeat to controller
	 * This will need to update metadata of chunk on controller
	 */
	public synchronized void receiveHeartBeat(HeartBeat heartbeat) {
		
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
