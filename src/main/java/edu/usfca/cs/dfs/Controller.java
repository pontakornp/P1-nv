package edu.usfca.cs.dfs;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import edu.usfca.cs.dfs.config.Config;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Controller {
	private String controllerNodeAddr;
	private int controllerNodePort;
	private ConcurrentHashMap<String, StorageMessages.StorageNode> activeStorageNodes;
	private ConcurrentHashMap<String, BloomFilter> bloomFilterMap;
	private ConcurrentHashMap<String, Timestamp> timeStamps;
	private static Integer BLOOM_FILTER_SIZE = 1024;
	private static Integer BLOOM_HASH_COUNT = 3;
	private static long MAX_STORAGE_TIME_INACTIVITY = 30000;

	private ConcurrentHashMap<String, List<StorageNode>> storageNodeReplicaMap;
	
	private static Controller controllerInstance; 
	
	private Controller() {
		
	}

	public ConcurrentHashMap<String, BloomFilter> getBloomFilters(){
		return this.bloomFilterMap;
	}
	
	public ConcurrentHashMap<String, StorageMessages.StorageNode> getActiveStorageNodes(){
		return this.activeStorageNodes;
	}

	public ConcurrentHashMap<String, List<StorageNode>> getStorageNodeReplicaMap() {
		return storageNodeReplicaMap;
	}
	
	public static Controller getInstance() {
		if (controllerInstance == null){
			System.out.println("New controller instance instantiated");
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
		System.out.println("Controller Node config updated.");
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
		System.out.println("Storage Node Received on controller for registration"
				+ storageNodeId +  " StorageNode Size: " + storageNode.getAvailableStorageCapacity());
		storageNode = this.getReplicationNodes(storageNode);
		if (!this.activeStorageNodes.containsKey(storageNodeId)) {
			this.activeStorageNodes.put(storageNodeId, storageNode);
			this.bloomFilterMap.put(storageNodeId, new BloomFilter(Controller.BLOOM_FILTER_SIZE, Controller.BLOOM_HASH_COUNT));
			this.timeStamps.put(storageNodeId, Controller.getCurrentTimeStamp());
			System.out.println("INFO: Storage Node registered with controller");
			this.printStorageNodeIds();
		}else {
			System.out.println("ERROR: Storage Node already registered with controller");
		}
	}
	
	public synchronized void printStorageNodeIds() {
		for (Map.Entry<String, StorageMessages.StorageNode> existingStorageNode : this.activeStorageNodes.entrySet()) {
			System.out.println(existingStorageNode.getValue().getStorageNodeId());
		}
	}
	
	/* This will update the replication nodes for storage nodes 
	 * This will randomly select two other registered nodes as replicas for current node
	 */
	private StorageMessages.StorageNode getReplicationNodes(StorageMessages.StorageNode storageNode) {
		ArrayList<String> duplicateNodeList = new ArrayList<String>(); 
		int count = 0;
		for (Map.Entry<String, StorageMessages.StorageNode> existingStorageNode : this.activeStorageNodes.entrySet()) {
			if(existingStorageNode.getKey() != storageNode.getStorageNodeId()){
				duplicateNodeList.add(existingStorageNode.getKey());
				count++;
			}
			if(count==2) {
				break;
			}
		}
		storageNode.toBuilder().addAllReplicationNodeIds(duplicateNodeList);
		
		return storageNode;
	}
	
	/*
	 * This will be called when StorageNode send HeartBeat to controller
	 * This will need to update metadata of chunk on controller
	 */
	public synchronized void receiveHeartBeat(String storageNodeId) {
		if(this.timeStamps.containsKey(storageNodeId)) {
			this.timeStamps.put(storageNodeId, Controller.getCurrentTimeStamp());
			System.out.println("INFO: Storage Node Heartbeat has been updated");
		}else {
			System.out.println("ERROR: Storage Node not registered on controller. Heartbeat will not be updated");
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
	public synchronized StorageMessages.ChunkMapping getNodesForChunkSave(StorageMessages.Chunk chunk) {
		String bloomFilterKey = chunk.getFileName() + "_" + chunk.getChunkId();
		System.out.println("Bloom Key: " + bloomFilterKey);
		ArrayList<StorageMessages.StorageNode> storageNodeList = this.findNodesContainingFileChunk(chunk, bloomFilterKey);
		return HDFSMessagesBuilder.constructChunkMapping(chunk, storageNodeList);
	}
	
	
	/*
	 * This splits the storagenodes into containing nodes and not containing nodes
	 * Adds one not containing node to ensure atleast one non containing node is returned
	 * This ensures the handling of storage node for files with different versions and bloom filter false positives
	 */
	public ArrayList<StorageMessages.StorageNode> findNodesContainingFileChunk(StorageMessages.Chunk chunk, String bloomKey){
		ArrayList<StorageMessages.StorageNode> containingStorageNodeList = new ArrayList<StorageMessages.StorageNode>();
		ArrayList<StorageMessages.StorageNode> notContainingStorageNodeList = new ArrayList<StorageMessages.StorageNode>();
		
		System.out.println("Current bloom filter map size" + this.bloomFilterMap.size());
		System.out.println("Chunk size: " + chunk.getChunkSize());
		for (Map.Entry<String, BloomFilter> storageNodeBloomFilter : this.bloomFilterMap.entrySet()) {
			StorageMessages.StorageNode storageNode = this.activeStorageNodes.get(storageNodeBloomFilter.getKey());
			System.out.println("Iterating bloom filter for storage node" + storageNode.getStorageNodeId());
			System.out.println("Current capacity of storagenode: " + +storageNode.getAvailableStorageCapacity());
			if(storageNodeBloomFilter.getValue().getBloomKey(bloomKey.getBytes())) {
				containingStorageNodeList.add(storageNode);
			}else if(storageNode.getAvailableStorageCapacity()-chunk.getChunkSize()>0) {
				notContainingStorageNodeList.add(storageNode);
			}
		}
		if(notContainingStorageNodeList.size()>0) {
			containingStorageNodeList.add(notContainingStorageNodeList.get(0));
		}
		System.out.println("StorageNodes Identified for chunk: " + String.valueOf(containingStorageNodeList.size()));
		return containingStorageNodeList;
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
				System.out.println("Identified StorageNode  inactivity detected for StorageNodeId: "+  storageNodeTimestamp.getKey());
				controller.timeStamps.remove(storageNodeTimestamp.getKey());
				// TODO: Need to handle recovery of node here
				controller.activeStorageNodes.remove(storageNodeTimestamp.getKey());
			}
		}
	}
	
	/*
	 * This will handle the replication of inactive node data and replicate it to another node
	 * This needs to identify the list of chunks on the inactive node
	 * for each chunk on inactive node it replicates the chunk to a new node
	 * 
	 */
	public synchronized void handleInactiveStorageNode() {
		
	}
	
	public synchronized void detectFileExistence(String fileName) {
		
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
              .option(ChannelOption.SO_BACKLOG, 128)
              .childOption(ChannelOption.SO_KEEPALIVE, true);
 
            ChannelFuture f = b.bind(this.controllerNodePort).sync();
            System.out.println("Controller started at given port: " + String.valueOf(this.controllerNodePort));
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
			//controllerNode.handleInactiveNodes();
			controllerNode.start();
		}catch (Exception e){
			System.out.println("Unable to start controller node");
			e.printStackTrace();
		}
		
    } 
}
