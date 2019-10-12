package edu.usfca.cs.dfs;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
	private ConcurrentHashMap<String, BloomFilter> bloomFilterList;
	private ConcurrentHashMap<String, Timestamp> timeStamps;
	private static Integer BLOOM_FILTER_SIZE = 1024;
	private static Integer BLOOM_HASH_COUNT = 3;
	
	private static Controller controllerInstance; 
	
	private Controller() {
		
	}
	
	public static Controller getInstance() {
		if (controllerInstance == null){
			controllerInstance = new Controller();
		}
		return controllerInstance;
	}
	
	private void setVariables(Config config) {
		this.controllerNodeAddr = config.getControllerNodeAddr();
		this.controllerNodePort = config.getControllerNodePort();
		this.activeStorageNodes = new ConcurrentHashMap<String, StorageMessages.StorageNode>();
		this.bloomFilterList = new ConcurrentHashMap<String, BloomFilter>();
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
		storageNode = this.getReplicationNodes(storageNode);
		if (!this.activeStorageNodes.containsKey(storageNodeId)) {
			this.activeStorageNodes.put(storageNodeId, storageNode);
			this.bloomFilterList.put(storageNodeId, new BloomFilter(Controller.BLOOM_FILTER_SIZE, Controller.BLOOM_HASH_COUNT));
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
		storageNode.newBuilder().addAllReplicationNodeIds(duplicateNodeList);
		
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
	}
	
	/*
	 *  This will be called by client to get the list of nodes to save a chunk
	 *  This will randomly select three nodes from activeStorageNodes
	 */
	public synchronized ArrayList<StorageMessages.StorageNode>getNodesForChunkSave() {
		ArrayList<StorageMessages.StorageNode> nodeList = new ArrayList<StorageMessages.StorageNode>();
		List<String> keysAsArray = new ArrayList<String>(this.activeStorageNodes.keySet());
		for (int i = 0; i < 3; i++) {
			Random r = new Random();
			nodeList.add(this.activeStorageNodes.get(keysAsArray.get(r.nextInt(keysAsArray.size()))));
		}
		return nodeList;
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
			controllerNode.start();
		}catch (Exception e){
			System.out.println("Unable to start controller node");
			e.printStackTrace();
		}
		
    } 
}
