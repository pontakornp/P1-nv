package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;

import edu.usfca.cs.dfs.Controller;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageNode;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class InboundHandler
extends SimpleChannelInboundHandler<StorageMessages.MessageWrapper> {

    public InboundHandler() { }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.MessageWrapper msg) {
    	int messageType = msg.getMessageType();
    	if (messageType == 1) {
    		StorageMessages.StorageNode storageNodeMsg = msg.getStorageNodeMsg();
    		StorageNode storageNode = new StorageNode();
    		storageNode.updateValuesFromProto(storageNodeMsg);
    		Controller controller = Controller.getInstance();
    		controller.addStorageNode(storageNode);
    	}else if(messageType == 2){
    		
    	}else if(messageType == 3){
    		
    	}
        StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();
        System.out.println("Storing file name: "
                + storeChunkMsg.getFileName());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
