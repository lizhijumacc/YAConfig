package com.yaconfig.server;


import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.yaconfig.commands.PutCommand;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class YAConfigServer implements Runnable{
	
	private int port;
	
	ConcurrentHashMap<String,Channel> channels;
	
	YAMessageQueue boardcastQueue;
	
	private UnPromisedMessages unPomisedMsgs;
	
	Thread boardcastThread;
	
	private YAConfig yaconfig;
	
	public YAConfigServer(int port,YAConfig yaconfig){
		this.port = port;
		channels = new ConcurrentHashMap<String,Channel>();
		boardcastQueue = new YAMessageQueue();
		this.yaconfig = yaconfig;
		unPomisedMsgs = new UnPromisedMessages(this);
	}
	
	public void countPromise(YAMessage msg){
		this.unPomisedMsgs.countPromise(msg);
	}
	
	@Override
	public void run(){
		boardcastThread = new Thread("boardcastThread"){
			@Override
			public void run(){
				while(true){
					YAMessage yamsg = null;
					try {
						yamsg = boardcastQueue.take();
						if(yamsg.type == YAMessage.Type.PUT){
							unPomisedMsgs.push(yamsg);
						}
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					
					for(Entry<String,Channel> c :channels.entrySet()){
						Channel channel = c.getValue();
						if(null != channel && channel.isActive()
								&& yamsg != null){
							channel.writeAndFlush(yamsg);
						}
					}
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		
		boardcastThread.start();
		
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try{

			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup)
			 .channel(NioServerSocketChannel.class)
			 .childHandler(new ChannelInitializer<SocketChannel>(){

				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast(
							//new LengthFieldPrepender(2),
							new ObjectEncoder(),
							//new LengthFieldBasedFrameDecoder(65535,0,2,0,2),
							new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
			    			new YAConfigServerHandler(yaconfig.server)
			    			);
				}
				 
			 })
			 .option(ChannelOption.SO_BACKLOG, 128)
			 .option(ChannelOption.SO_REUSEADDR, true)
			 .childOption(ChannelOption.SO_KEEPALIVE, true)
			 .childOption(ChannelOption.TCP_NODELAY, true)
			 .childOption(ChannelOption.SO_REUSEADDR, true);
			
			doBind(b);
			
		}finally{
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

	public void doBind(ServerBootstrap b){
		ChannelFuture f;
		try {
			f = b.bind(port).sync().addListener(new ChannelFutureListener(){

				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if(!future.isSuccess()){
						System.out.println("start server error");
					}else{
						System.out.println("YAConfig server is started on port:" + port);
					}
				}
				
			});
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void addChannel(Channel c){
		if(null == c || !c.isActive()){
			return;
		}
		InetSocketAddress address = (InetSocketAddress)c.remoteAddress();
		this.channels.putIfAbsent(
				address.getHostName()+ ":" + String.valueOf(address.getPort()), c);
	}
	
	public void broadcastToQuorums(YAMessage msg) {
		try {
			boardcastQueue.push(msg);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public void processMessage(YAMessage yamsg) {
		if(yamsg.type == YAMessage.Type.PUT){
			proposal(yamsg);
		}else if(yamsg.type == YAMessage.Type.PUT_NOPROMISE){
			PutCommand put = new PutCommand("put");
			put.setExecutor(yaconfig.exec);
			put.execute(yamsg.key,yamsg.value,true);			
		}else if(yamsg.type == YAMessage.Type.GET){
			//TODO
		}else if(yaconfig.statusEquals(EndPoint.Status.LEADING)
				&& yamsg.type == YAMessage.Type.PROMISE){
			countPromise(yamsg);
		}
	}

	private void proposal(YAMessage yamsg) {
		PutCommand put = new PutCommand("put");
		put.setExecutor(yaconfig.exec);
		put.execute(yamsg.key,yamsg.value,false);
	}

	public void removeChannel(Channel channel) {
		InetSocketAddress address = (InetSocketAddress)channel.remoteAddress();
		channels.remove(address.getHostName()+ ":" + String.valueOf(address.getPort()));
	}

	public void setVID(String serverID, Long sequenceNum) {
		yaconfig.setVID(serverID,sequenceNum);
	}
	
}
