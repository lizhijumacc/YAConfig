package com.yaconfig.server;


import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
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
	
	Thread boardcastThread;
	
	public YAConfigServer(int port){
		this.port = port;
		channels = new ConcurrentHashMap<String,Channel>();
		boardcastQueue = new YAMessageQueue();
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
			    			new YAConfigServerHandler(YAConfig.server)
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
		YAConfig.processMessage(yamsg);
	}

	public void removeChannel(Channel channel) {
		InetSocketAddress address = (InetSocketAddress)channel.remoteAddress();
		channels.remove(address.getHostName()+ ":" + String.valueOf(address.getPort()));
	}
	
}
