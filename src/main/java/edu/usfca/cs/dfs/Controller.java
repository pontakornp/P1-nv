package edu.usfca.cs.dfs;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.StorageMessages.MessageWrapper;
import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Controller {
	static Logger logger = LogManager.getLogger(Controller.class);
	
	private String controllerNodeAddr;
	private int controllerNodePort;
	private ConcurrentHashMap<String, StorageMessages.StorageNode> activeStorageNodes;
	private ConcurrentHashMap<String, BloomFilter> bloomFilterMap;
	private ConcurrentHashMap<String, Timestamp> timeStamps;
	private static Integer BLOOM_FILTER_SIZE = 1024;
	private static Integer BLOOM_HASH_COUNT = 3;
	private static long MAX_STORAGE_TIME_INACTIVITY = 10000;
	private static int MAX_REPLICAS = 1;

	private static Controller controllerInstance; 
	
	private Controller() {
		
	}

	public ConcurrentHashMap<String, BloomFilter> getBloomFilters(){
		return this.bloomFilterMap;
	}
	
	public ConcurrentHashMap<String, StorageMessages.StorageNode> getActiveStorageNodesMap(){
		return this.activeStorageNodes;
	}

	public static Controller getInstance() {
		if (controllerInstance == null){
			logger.info("New controller instance instantiated");
			controllerInstance = new Controller();
		}
		return controllerInstance;
	}
	
	private void setVariables(Config config) {
		this.controllerNodeAddr = config.getControllerNodeAddr();
		this.controllerNodePort = config.getControllerNodePort();
		this.activeStorageNodes = new ConcurrentHashMap<String, StorageMessages.StorageNode>();
		this.bloomFilterMap = new ConcurrentHashMap<String, BloomFilter>();
		this.timeStamps = new ConcurrentHashMap<String, Timestamp>();
		logger.info("Controller Node config updated.");
	}
	
	private static Timestamp getCurrentTimeStamp() {
		Date date= new Date();
		long time = date.getTime();
		Timestamp ts = new Timestamp(time);
		return ts;
	}

	/*
	 * This is called during registration of StorageNode
	 * This will add the node metadata to activeStorageNodes
	 * Takes the Storage 
	 */
	public synchronized void addStorageNode(StorageMessages.StorageNode storageNode) {
		String storageNodeId = storageNode.getStorageNodeId();
		logger.info("Storage Node Received on controller for registration"
				+ storageNodeId +  " StorageNode Size: " + storageNode.getAvailableStorageCapacity());
		
		if (!this.activeStorageNodes.containsKey(storageNodeId)) {
			this.activeStorageNodes.put(storageNodeId, storageNode);
			this.bloomFilterMap.put(storageNodeId, new BloomFilter(Controller.BLOOM_FILTER_SIZE, Controller.BLOOM_HASH_COUNT));
			this.timeStamps.put(storageNodeId, Controller.getCurrentTimeStamp());
			logger.info("Storage Node registered with controller");
		}else {
			logger.error("Storage Node already registered with controller");
		}
		
		this.getUpdatedNodeList();
	}
	
	public void printStorageNodes() {
		logger.info("Current State of Storage Nodes");
		for (Map.Entry<String, StorageMessages.StorageNode> activeStorageNode : this.activeStorageNodes.entrySet()) {
			logger.info(activeStorageNode.toString());
		}
	}
	
	public synchronized List<StorageMessages.StorageNode> getActiveStorageNodes() {
		List<StorageMessages.StorageNode> storageNodeList = new ArrayList<StorageMessages.StorageNode>();
		for(StorageMessages.StorageNode activeStorageNode : this.activeStorageNodes.values()) {
			storageNodeList.add(activeStorageNode);
		}
		return storageNodeList;
	}
	
	/*
	 * Updates the replica nodes of existing nodes
	 * Generates a max priority queue for storage nodes based on available storage capacity
	 * Adds replica nodes for storage nodes which have replica count less than max replica count 
	 */
	
	private void getUpdatedNodeList(){
		List<StorageMessages.StorageNode> updatedNodeList = new  ArrayList<StorageMessages.StorageNode>();
		
		if(this.activeStorageNodes.values().size()>1) {
			int maxStorageNodesCount = this.activeStorageNodes.values().size()-1;
			int maxReplicas = Math.min(maxStorageNodesCount, Controller.MAX_REPLICAS);
			HashMap<String, ArrayList<String>> existingReplicaMap = new HashMap<String, ArrayList<String>>();
			
			PriorityQueue<StorageMessages.StorageNode> completeNodeListPQ 
				= new PriorityQueue<StorageMessages.StorageNode>(maxStorageNodesCount+1, new Comparator<StorageMessages.StorageNode>() {
					@Override 
					public int compare(StorageMessages.StorageNode p1, StorageMessages.StorageNode p2) {
			            return (int)p2.getAvailableStorageCapacity() - (int)p1.getAvailableStorageCapacity(); // Descending
			        }
				});
			
			for (StorageMessages.StorageNode existingStorageNode : this.activeStorageNodes.values()) {
				completeNodeListPQ.add(existingStorageNode);
				
				ArrayList<String> existingReplicaNodeIdList = new ArrayList<String>();
				for(StorageMessages.ReplicaNode replicaNode: existingStorageNode.getReplicaNodesList()) {
					existingReplicaNodeIdList.add(replicaNode.getStorageNodeId());
				}
				existingReplicaMap.put(existingStorageNode.getStorageNodeId(), existingReplicaNodeIdList);
			}
			
			for (StorageMessages.StorageNode activeStorageNode : this.activeStorageNodes.values()) {
				String storageNodeId = activeStorageNode.getStorageNodeId();
				
				if(activeStorageNode.getReplicaNodesList().size()<maxReplicas) {
					ArrayList<StorageMessages.ReplicaNode> outputNodeList = new ArrayList<StorageMessages.ReplicaNode>();
					ArrayList<StorageMessages.StorageNode> usedNodeList = new ArrayList<StorageMessages.StorageNode>();
					int newReplicaCount = maxReplicas-existingReplicaMap.get(storageNodeId).size();
					
					for(int i=0; i<newReplicaCount; i++) {
						
						StorageMessages.StorageNode currentNode = completeNodeListPQ.peek();
						while(storageNodeId == currentNode.getStorageNodeId()
								|| existingReplicaMap.get(storageNodeId).contains(currentNode.getStorageNodeId())){
							usedNodeList.add(completeNodeListPQ.remove());
							currentNode = completeNodeListPQ.peek();
						}
						logger.info("Storage Node: " + currentNode.getStorageNodeId() + " added as replica for " + activeStorageNode.getStorageNodeId());
						StorageMessages.StorageNode tempSN = completeNodeListPQ.remove();
						usedNodeList.add(tempSN);
						StorageMessages.ReplicaNode replicaNodeMsg 
							= StorageMessages.ReplicaNode.newBuilder()
							.setStorageNodeId(tempSN.getStorageNodeId())
							.setStorageNodeAddr(tempSN.getStorageNodeAddr())
							.setStorageNodePort(tempSN.getStorageNodePort())
							.build();
						outputNodeList.add(replicaNodeMsg);
					}
					
					// Re-add nodes which were removed from node list
					completeNodeListPQ.addAll(usedNodeList);
					
					activeStorageNode = activeStorageNode.toBuilder().addAllReplicaNodes(outputNodeList).build();
					this.activeStorageNodes.put(storageNodeId, activeStorageNode);
					updatedNodeList.add(activeStorageNode);
				}
			}
			for(StorageMessages.StorageNode updatedStorageNode: updatedNodeList) {
				logger.info(updatedStorageNode.toString());
			}
			this.updateReplicaNodesByMaxAvailableSize(updatedNodeList);
		}
		this.printStorageNodes();
	}
	
	/* This will update the replication nodes for storage nodes 
	 * This will select other registered nodes as replicas for current node
	 * Implemented a priority queue which compares(desc) by available storage capacity of node
	 */
	private void updateReplicaNodesByMaxAvailableSize(List<StorageMessages.StorageNode> updatedNodeList) {
		
		Thread thread = new Thread() {
			public void run() {
				for(StorageMessages.StorageNode updatedNode: updatedNodeList) {
					try {
						EventLoopGroup workerGroup = new NioEventLoopGroup();
				        MessagePipeline pipeline = new MessagePipeline();
				        
				        logger.info("Update replica node initiated from controller to  storage node : " + 
				        		updatedNode.getStorageNodeAddr() + String.valueOf(updatedNode.getStorageNodePort()));
				        Bootstrap bootstrap = new Bootstrap()
				            .group(workerGroup)
				            .channel(NioSocketChannel.class)
				            .option(ChannelOption.SO_KEEPALIVE, true)
				            .handler(pipeline);
				        
				        ChannelFuture cf = bootstrap.connect(updatedNode.getStorageNodeAddr(), updatedNode.getStorageNodePort());
				        cf.syncUninterruptibly();
				
				        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRegisterNodeResponse(updatedNode);
				
				        Channel chan = cf.channel();
				        chan.writeAndFlush(msgWrapper);
				        logger.info("Update replica node completed from controller to storage Node: " + 
				        		updatedNode.getStorageNodeAddr() + String.valueOf(updatedNode.getStorageNodePort()));
				        chan.closeFuture().sync();
				        workerGroup.shutdownGracefully();
					} catch (Exception e) {
						e.printStackTrace();
						logger.error("Update replica node initiated from controller to storage Node failed. Storage node connection establishment failed");
					}
				}
			}
		};
		thread.start();
	}
	
	/*
	 * This will be called when StorageNode send HeartBeat to controller
	 * This will need to update metadata of chunk on controller
	 */
	public synchronized void receiveHeartBeat(StorageMessages.StorageNode storageNode) {
		String storageNodeId = storageNode.getStorageNodeId();
		
		if(this.timeStamps.containsKey(storageNodeId)) {
			this.timeStamps.put(storageNodeId, Controller.getCurrentTimeStamp());
			StorageMessages.StorageNode activeStorageNode = this.activeStorageNodes.get(storageNodeId);
			activeStorageNode = activeStorageNode.toBuilder()
					.setAvailableStorageCapacity(storageNode.getAvailableStorageCapacity())
					.build();
			this.activeStorageNodes.put(storageNodeId, activeStorageNode);
		}else {
			logger.error("Storage Node not registered on controller. Heartbeat will not be updated");
		}
	}
	
	/*
	 * This is called when controller deletes StorageNode when it decides it on inactive status
	 * This will remove the node from activeStorageNodes
	 */
	public synchronized void deleteStorageNode(StorageNode storageNode){
		this.activeStorageNodes.remove(storageNode.getStorageNodeId());
		this.bloomFilterMap.remove(storageNode.getStorageNodeId());
		this.timeStamps.remove(storageNode.getStorageNodeId());
	}

	/*
	 *  This will be called by client to get the list of nodes to save a chunk
	 *  This will check if chunk exists and identifies storagenodes depending on its existence
	 *  if file chunk exists return the node containing the previous version chunk
	 *  if file chunk does not exist return new storage node that can save the file chunk 
	 */
	public synchronized ArrayList<StorageMessages.ChunkMapping> getNodesForChunkSave(List<StorageMessages.Chunk> chunkList) {
		PriorityQueue<StorageMessages.StorageNode> completeNodeListPQ 
			= new PriorityQueue<StorageMessages.StorageNode>(this.activeStorageNodes.keySet().size(), new Comparator<StorageMessages.StorageNode>() {
			@Override 
			public int compare(StorageMessages.StorageNode p1, StorageMessages.StorageNode p2) {
	            return (int)p2.getAvailableStorageCapacity() - (int)p1.getAvailableStorageCapacity(); // Descending
	        }
		});
	
		for (StorageMessages.StorageNode existingStorageNode : this.activeStorageNodes.values()) {
			completeNodeListPQ.add(existingStorageNode);
		}
		ArrayList<StorageMessages.ChunkMapping> chunkMappingList = new ArrayList<StorageMessages.ChunkMapping>();
		for (StorageMessages.Chunk chunk : chunkList) {
			ArrayList<StorageMessages.StorageNode> storageNodeList = this.findNodesContainingFileChunk(chunk, completeNodeListPQ);
			StorageMessages.ChunkMapping chunkMapping = HDFSMessagesBuilder.constructChunkMapping(chunk, storageNodeList);
			chunkMappingList.add(chunkMapping);
		}
		return chunkMappingList;
	}

	/*
	 * This splits the storagenodes into containing nodes and not containing nodes
	 * Adds one not containing node to ensure atleast one non containing node is returned
	 * This ensures the handling of storage node for files with different versions and bloom filter false positives
	 */
	public ArrayList<StorageMessages.StorageNode> findNodesContainingFileChunk(StorageMessages.Chunk chunk, PriorityQueue<StorageMessages.StorageNode> completeNodeListPQ){
		String bloomFilterKey = chunk.getFileName() + "_" + chunk.getChunkId();
		ArrayList<StorageMessages.StorageNode> containingStorageNodeList = new ArrayList<StorageMessages.StorageNode>();
		ArrayList<StorageMessages.StorageNode> notcontainingStorageNodeList = new ArrayList<StorageMessages.StorageNode>();
		boolean primaryNode = true;
		
		for (Map.Entry<String, BloomFilter> storageNodeBloomFilter : this.bloomFilterMap.entrySet()) {
			StorageMessages.StorageNode storageNode = this.activeStorageNodes.get(storageNodeBloomFilter.getKey());
			if(storageNodeBloomFilter.getValue().getBloomKey(bloomFilterKey.getBytes())) {
				containingStorageNodeList.add(storageNode);
				logger.info("Storagenode Identified containing bloom key: " 
						+ bloomFilterKey + " " + storageNode.getStorageNodeId());
			}else if(primaryNode){
				if(completeNodeListPQ.peek()!=null && completeNodeListPQ.peek().getAvailableStorageCapacity()>=chunk.getChunkSize()){
					StorageMessages.StorageNode newStorageNode = completeNodeListPQ.remove();
					notcontainingStorageNodeList.add(newStorageNode);
					
					StorageMessages.StorageNode modifiedStorageNode 
						= StorageMessages.StorageNode.newBuilder()
						.setStorageNodeId(newStorageNode.getStorageNodeId())
		                .setStorageNodeAddr(newStorageNode.getStorageNodeAddr())
		                .setStorageNodePort(newStorageNode.getStorageNodePort())
		                .setAvailableStorageCapacity(newStorageNode.getAvailableStorageCapacity()-chunk.getChunkSize())
		                .setMaxStorageCapacity(newStorageNode.getMaxStorageCapacity())
		                .addAllReplicaNodes(newStorageNode.getReplicaNodesList())
		                .build();
					
					logger.info("New Storagenode Identified for new bloom key: " + bloomFilterKey + storageNode.getStorageNodeId());
					completeNodeListPQ.add(modifiedStorageNode);
				}
				primaryNode = false;
			}
		}
		// This is added so that we can send atleast one node to save assuming false positives
		if(notcontainingStorageNodeList.size()>0 && containingStorageNodeList.size()==0) {
			containingStorageNodeList.add(notcontainingStorageNodeList.get(0));
		}
		
		logger.info("StorageNodes Count Identified for chunk: " + String.valueOf(containingStorageNodeList.size()));
		return containingStorageNodeList;
	}
	
	public boolean getChunkMappingForRetreive(String fileName, int chunkNumber, List<StorageMessages.ChunkMapping> chunkMappingList) {
		String bloomFilterKey = fileName + "_" + chunkNumber;
		ArrayList<StorageMessages.StorageNode> storageNodeList = new ArrayList<StorageMessages.StorageNode>();
		for (Map.Entry<String, BloomFilter> storageNodeBloomFilter : this.bloomFilterMap.entrySet()) {
			StorageMessages.StorageNode storageNode = this.activeStorageNodes.get(storageNodeBloomFilter.getKey());
			if(storageNodeBloomFilter.getValue().getBloomKey(bloomFilterKey.getBytes())) {
				storageNodeList.add(storageNode);
			}
		}
		// if no storage node potentially contain the file, return null
		if(storageNodeList.isEmpty()) {
			return false;
		}
		logger.info("Identified storage nodes containing the chunk" + bloomFilterKey);
		logger.info(storageNodeList.toString());
		
		StorageMessages.Chunk chunk = StorageMessages.Chunk.newBuilder()
				.setFileName(fileName)
				.setChunkId(chunkNumber)
				.build();
		StorageMessages.ChunkMapping chunkMapping = StorageMessages.ChunkMapping.newBuilder()
				.setChunk(chunk)
				.addAllStorageNodeObjs(storageNodeList)
				.build();
		chunkMappingList.add(chunkMapping);
		return true;
	}

	// get storage nodes from controller
	public synchronized List<StorageMessages.ChunkMapping> getNodesForRetrieveFile(String fileName, int maxChunkNumber) {
		// traverse the chunkId and put those chunk and list of storage nodes in Chunk Mapping
		// key = chunk, value = list of storage nodes
		logger.info("Client gets nodes for retrieve file from client");
		List<StorageMessages.ChunkMapping> chunkMappingList = new ArrayList<StorageMessages.ChunkMapping>();
		for(int i = 0; i < maxChunkNumber; i++) {
			if (!getChunkMappingForRetreive(fileName, i, chunkMappingList)) {
				return null;
			}
		}
		return chunkMappingList;
	}
	
	/*
	 * This will update the chunk save from storagenode on controller
	 * This will add the filename_chunkid to bloom filter of storageNode
	 * This will update the storageNode max Storage value on controller
	 */
	public synchronized void updateChunkSaveonController(StorageMessages.StorageNode storageNode, StorageMessages.Chunk chunk) {
		String bloomKey = chunk.getFileName() + "_" + chunk.getChunkId();
		String storageNodeId = storageNode.getStorageNodeId();
		this.bloomFilterMap.get(storageNodeId).putBloomKey(bloomKey.getBytes());
		this.activeStorageNodes.put(storageNodeId, storageNode);
		logger.info(bloomKey + " : Bloom key added on controller with storageNodeid: " + storageNode.getStorageNodeId());
	}

	
	/*
	 * This will be called repeatedly and identify the list of inactive nodes
	 * for each inactive node it handles the inactive node by replicating the data on inactive node
	 */
	public synchronized void handleInactiveNodes() {
		Thread thread = new Thread() {
			public void run() {
				while(true) {
					try {
						System.out.println("Handling Inactive Nodes thread running");
						Controller.detectInactiveNodes(Controller.getInstance());
						System.out.println("Handling Inactive Nodes thread sleeping");
			            Thread.sleep(30000);
			        } catch (InterruptedException e) {
			            e.printStackTrace();
			        }
				}
			}
		};
		thread.start();
	}
	
	
	/*
	 * This will be called to detect list of inactive nodes
	 */
	private static void detectInactiveNodes(Controller controller) {
		for (Map.Entry<String, Timestamp> storageNodeTimestamp : controller.timeStamps.entrySet()) {
			if(Controller.getCurrentTimeStamp().getTime()-storageNodeTimestamp.getValue().getTime()>=Controller.MAX_STORAGE_TIME_INACTIVITY){
				logger.info("Identified StorageNode  inactivity detected for StorageNodeId: "+  storageNodeTimestamp.getKey());
				controller.handleInactiveStorageNode(controller.activeStorageNodes.get(storageNodeTimestamp.getKey()));
			}
		}
	}
	
	/*
	 * This will handle the replication of inactive node data and replicate it to another node
	 * This needs to identify the list of chunks on the inactive node
	 * for each chunk on inactive node it replicates the chunk to a new node
	 * 
	 */
	public void handleInactiveStorageNode(StorageMessages.StorageNode inactiveStorageNode) {
		String inactiveStorageNodeId = inactiveStorageNode.getStorageNodeId();
		this.timeStamps.remove(inactiveStorageNodeId);
		this.activeStorageNodes.remove(inactiveStorageNodeId);
		this.bloomFilterMap.remove(inactiveStorageNodeId);
		
		long availableCapacity = inactiveStorageNode.getAvailableStorageCapacity();
		long maxCapacity = inactiveStorageNode.getMaxStorageCapacity();
		long requiredSpace = maxCapacity - availableCapacity;
		
		List<String> inactivereplicaNodeList = new ArrayList<String>();
		List<String> inactivePrimaryNodeList = new ArrayList<String>();
		List<String> notIncludedList = new ArrayList<String>();
		
		// Replica nodes of inactive node cannot be chosen as a new replica
		for (StorageMessages.ReplicaNode replicaNode : inactiveStorageNode.getReplicaNodesList()){
			inactivereplicaNodeList.add(replicaNode.getStorageNodeId());
			notIncludedList.add(replicaNode.getStorageNodeId());
		}
		logger.info("ReplicaNodeList");
		logger.info(Arrays.toString(inactivereplicaNodeList.toArray()));
		
		
		// Primary nodes which stored replicas on inactive nodes cannot be chosen as new replica
		for (String storageNodeId: this.activeStorageNodes.keySet()) {
			StorageMessages.StorageNode storageNode = this.activeStorageNodes.get(storageNodeId);
			List<StorageMessages.ReplicaNode> replicaNodeList = storageNode.getReplicaNodesList();
			
			for (StorageMessages.ReplicaNode replicaNode : replicaNodeList){
				if (replicaNode.getStorageNodeId().equals(inactiveStorageNodeId)) {
					logger.info(storageNodeId + "has its replica stored on failed node " + inactiveStorageNodeId);
					inactivePrimaryNodeList.add(storageNodeId);
					if(!notIncludedList.contains(storageNodeId)) {
						notIncludedList.add(storageNodeId);
					}
					break;
				}
			} 
		}
		
		logger.info("PrimaryNodeList");
		logger.info(Arrays.toString(inactivePrimaryNodeList.toArray()));
		
		logger.info("Not Included List");
		logger.info(Arrays.toString(notIncludedList.toArray()));
		
		StorageMessages.StorageNode replacedNode=null;
		// New Node to replace failed node
		for (String storageNodeId: this.activeStorageNodes.keySet()) {
			if(!notIncludedList.contains(storageNodeId)) {
				StorageMessages.StorageNode newStorageNode = this.activeStorageNodes.get(storageNodeId);
				if(newStorageNode.getAvailableStorageCapacity()>=requiredSpace) {
					replacedNode = newStorageNode;
					break;
				}
			}
		}
		
		if(replacedNode!=null) {
			logger.info("Replaced Node");
			logger.info(replacedNode.toString());
			StorageMessages.ReplicaNode replacedReplicaNode 
				= StorageMessages.ReplicaNode.newBuilder()
					.setStorageNodeAddr(replacedNode.getStorageNodeAddr())
					.setStorageNodeId(replacedNode.getStorageNodeId())
					.setStorageNodePort(replacedNode.getStorageNodePort())
					.build();
			
			List<StorageMessages.StorageNode> updatedNodeList = new  ArrayList<StorageMessages.StorageNode>();
			for(String inactivePrimaryNodeId: inactivePrimaryNodeList) {
				StorageMessages.StorageNode inactivePrimaryNode = this.activeStorageNodes.get(inactivePrimaryNodeId);
				List<StorageMessages.ReplicaNode> replicaNodeList = inactivePrimaryNode.getReplicaNodesList();
				List<StorageMessages.ReplicaNode> updatedReplicaNodeList = new ArrayList<StorageMessages.ReplicaNode>();
				for (StorageMessages.ReplicaNode replicaNode : replicaNodeList){
					if (!replicaNode.getStorageNodeId().equals(inactiveStorageNodeId)) {
						updatedReplicaNodeList.add(replicaNode);
					}
				}
				updatedReplicaNodeList.add(replacedReplicaNode);
				
				StorageMessages.StorageNode editedStorageNode 
					= StorageMessages.StorageNode.newBuilder()
						.setStorageNodeId(inactivePrimaryNode.getStorageNodeId())
						.setStorageNodeAddr(inactivePrimaryNode.getStorageNodeAddr())
						.setStorageNodePort(inactivePrimaryNode.getStorageNodePort())
						.setAvailableStorageCapacity(inactivePrimaryNode.getAvailableStorageCapacity())
						.setMaxStorageCapacity(inactivePrimaryNode.getMaxStorageCapacity())
						.addAllReplicaNodes(updatedReplicaNodeList)
						.build();
				
				updatedNodeList.add(editedStorageNode);
				this.activeStorageNodes.put(inactivePrimaryNodeId, editedStorageNode);
			}
			
			for(String inactivereplicaNode: inactivereplicaNodeList) {
				StorageMessages.StorageNode inactiveReplicaNode = this.activeStorageNodes.get(inactivereplicaNode);
				List<StorageMessages.ReplicaNode> replicaNodeList = inactiveReplicaNode.getReplicaNodesList();
				List<StorageMessages.ReplicaNode> newReplicaNodeList = new ArrayList<StorageMessages.ReplicaNode>();
				for (StorageMessages.ReplicaNode replicaNode : replicaNodeList){
					newReplicaNodeList.add(replicaNode);
				}
				newReplicaNodeList.add(replacedReplicaNode);
				
				StorageMessages.StorageNode editedStorageNode 
					= StorageMessages.StorageNode.newBuilder()
						.setStorageNodeId(inactiveReplicaNode.getStorageNodeId())
						.setStorageNodeAddr(inactiveReplicaNode.getStorageNodeAddr())
						.setStorageNodePort(inactiveReplicaNode.getStorageNodePort())
						.setAvailableStorageCapacity(inactiveReplicaNode.getAvailableStorageCapacity())
						.setMaxStorageCapacity(inactiveReplicaNode.getMaxStorageCapacity())
						.addAllReplicaNodes(newReplicaNodeList)
						.build();
				this.activeStorageNodes.put(inactivereplicaNode, editedStorageNode);
				List<StorageMessages.StorageNode> updatedReplicaNodeList = new  ArrayList<StorageMessages.StorageNode>();
				updatedReplicaNodeList.add(editedStorageNode);
				this.updateReplicaNodesByMaxAvailableSize(updatedReplicaNodeList);
				this.handlePrimaryNodeFailure(inactiveStorageNode, editedStorageNode, replacedNode);
				break;
			}
			
			logger.info("New storage node mappings");
			printStorageNodes();
			logger.info(updatedNodeList);
			this.updateReplicaNodesByMaxAvailableSize(updatedNodeList);
			this.handleReplicaNodeFailure(replacedNode, updatedNodeList);
			
		}
	}
	
	public void handlePrimaryNodeFailure(StorageMessages.StorageNode failedNode,
			StorageMessages.StorageNode failedNodeReplica, StorageMessages.StorageNode replacedNode) {
		try {
			EventLoopGroup workerGroup = new NioEventLoopGroup();
	        MessagePipeline pipeline = new MessagePipeline();
	        
	        logger.info("Recover all replica chunks of failed node initiated from controller to storage node : " + 
	        		failedNodeReplica.getStorageNodeAddr() + String.valueOf(failedNodeReplica.getStorageNodePort()));
	        Bootstrap bootstrap = new Bootstrap()
	            .group(workerGroup)
	            .channel(NioSocketChannel.class)
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .handler(pipeline);
	        
	        ChannelFuture cf = bootstrap.connect(failedNodeReplica.getStorageNodeAddr(), failedNodeReplica.getStorageNodePort());
	        cf.syncUninterruptibly();
	
	        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRecoverPrimaryFromNodeRequest(failedNode, replacedNode);
	
	        Channel chan = cf.channel();
	        chan.writeAndFlush(msgWrapper);
	        logger.info("Recover all replica chunks of failed node completed from controller to storage Node: " + 
	        		failedNodeReplica.getStorageNodeAddr() + String.valueOf(failedNodeReplica.getStorageNodePort()));
	        chan.closeFuture().sync();
	        workerGroup.shutdownGracefully();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Recover primary chunks initiated from controller to storage node failed. Storage node connection establishment failed");
		}
	}
	
	public void handleReplicaNodeFailure(StorageMessages.StorageNode replacedNode, List<StorageMessages.StorageNode> updatedNodeList) {
		logger.info("Handling replica node failure. Recovering primary chunks from primary nodes to new node");
		logger.info(updatedNodeList.toString());
		for(StorageMessages.StorageNode updatedNode: updatedNodeList) {
			try {
				EventLoopGroup workerGroup = new NioEventLoopGroup();
		        MessagePipeline pipeline = new MessagePipeline();
		        
		        logger.info("Recover primary chunks initiated from controller to storage node : " + 
		        		updatedNode.getStorageNodeAddr() + String.valueOf(updatedNode.getStorageNodePort()));
		        Bootstrap bootstrap = new Bootstrap()
		            .group(workerGroup)
		            .channel(NioSocketChannel.class)
		            .option(ChannelOption.SO_KEEPALIVE, true)
		            .handler(pipeline);
		        
		        ChannelFuture cf = bootstrap.connect(updatedNode.getStorageNodeAddr(), updatedNode.getStorageNodePort());
		        cf.syncUninterruptibly();
		
		        MessageWrapper msgWrapper = HDFSMessagesBuilder.constructRecoverReplicasFromNodeRequest(replacedNode);
		
		        Channel chan = cf.channel();
		        chan.writeAndFlush(msgWrapper);
		        logger.info("Recover primary chunks from node completed from controller to storage Node: " + 
		        		updatedNode.getStorageNodeAddr() + String.valueOf(updatedNode.getStorageNodePort()));
		        chan.closeFuture().sync();
		        workerGroup.shutdownGracefully();
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Recover primary chunks initiated from controller to storage node failed. Storage node connection establishment failed");
			}
		}
	}
	
	
	public void start() throws IOException, InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline();
        
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
              .channel(NioServerSocketChannel.class)
              .childHandler(pipeline)
              .option(ChannelOption.SO_BACKLOG, 2048)
              .childOption(ChannelOption.SO_KEEPALIVE, true);
 
            ChannelFuture f = b.bind(this.controllerNodePort).sync();
            logger.info("Controller started at given port: " + String.valueOf(this.controllerNodePort));
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
	
	
	public static void main(String[] args) throws IOException {
		String configFileName;
		if(args.length>0) {
    		configFileName= args[0];
    	}else {
    		configFileName = "config.json";
    	}
		
		Config config = new Config(configFileName);
		Controller controllerNode = Controller.getInstance();
		controllerNode.setVariables(config);
		try {
			controllerNode.handleInactiveNodes();
			controllerNode.start();
		}catch (Exception e){
			logger.error("Unable to start controller node. Address already in use");
			e.printStackTrace();
		}
		
    } 
}
