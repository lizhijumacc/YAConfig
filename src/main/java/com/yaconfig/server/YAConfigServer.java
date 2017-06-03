package com.yaconfig.server;


import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class YAConfigServer implements Runnable{
	
	private int port;
	
	ConcurrentHashMap<String,Channel> channels;
	
	YAMessageQueue boardcastQueue;
	
	YAMessageQueue systemMsgQueue;
	
	ExecutorService serverService;
	
	Runnable boardcastTask;
	
	Runnable sendSystemMsgTask;
	
	Runnable bindTask;
	
	private UnPromisedMessages unPomisedMsgs;
	
	private YAConfig yaconfig;
	
	public YAConfigServer(int port,YAConfig yaconfig){
		this.port = port;
		channels = new ConcurrentHashMap<String,Channel>();
		boardcastQueue = new YAMessageQueue();
		systemMsgQueue = new YAMessageQueue();
		this.yaconfig = yaconfig;
		unPomisedMsgs = new UnPromisedMessages(this);
		serverService = Executors.newCachedThreadPool();
	}
	
	public void countPromise(YAMessage msg){
		this.unPomisedMsgs.countPromise(msg);
	}
	
	@Override
	public void run(){
		boardcastTask = new Runnable(){
			@Override
			public void run(){
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
			}
		};
		
		sendSystemMsgTask = new Runnable(){
			@Override
			public void run(){
					YAMessage yamsg = null;
					try {
						yamsg = systemMsgQueue.take();
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
			}
		};
		
		bindTask = new Runnable(){
			@Override
			public void run(){
				doBind(port);
			}
		};
		
		Runnable clientServerBindTask = new Runnable(){
			@Override
			public void run(){
				if(port == 4247){
					doBind(8888);
				}
			}
		};
		
		serverService.execute(bindTask);
		
		serverService.execute(clientServerBindTask);
	}

	public void doBind(final int bindPort){
		ChannelFuture f;
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup)
			 .channel(NioServerSocketChannel.class)
			 .childHandler(new ChannelInitializer<SocketChannel>(){

				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast(
							//new LengthFieldPrepender(2),
							new IdleStateHandler(2,0,0,TimeUnit.SECONDS),
							new IdleHeartbeatHandler(yaconfig),
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
			
			f = b.bind(bindPort).sync().addListener(new ChannelFutureListener(){

				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if(!future.isSuccess()){
						System.out.println("start server error");
					}else{
						System.out.println("YAConfig server is started on port:" + bindPort);
					}
				}
				
			});
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally{
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
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
			if(isSystemMessage(msg)){
				systemMsgQueue.push(msg);
				serverService.execute(sendSystemMsgTask);
			}else{
				boardcastQueue.push(msg);
				serverService.execute(boardcastTask);
			}
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
	
	private boolean isSystemMessage(YAMessage msg) {
		return msg.key.indexOf("com.yacondfig") == 0;
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
