package com.yaconfig.server;

import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.yaconfig.storage.YAHashMap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

public class YAConfigClient implements Runnable{
	
	private EventLoopGroup loop = new NioEventLoopGroup();
	
	private ExecutorService connectService;
	
	private ExecutorService processMsgService;
	
	private ConcurrentHashMap<String,Channel> channels;
	
	YAMessageQueue rcvQueue;
	
	YAMessageQueue sendToMasterQueue;
	
	Thread sendToMasterThread;
	
	Thread clientRcvMessageThread;
	
	public YAConfigClient(){
		loop = new NioEventLoopGroup();
		//the number of endpoint is countable,use BIO read the 
		//quorums messages.
		connectService = Executors.newFixedThreadPool(YAConfig.quorums);
		channels = new ConcurrentHashMap<String,Channel>();
		rcvQueue = new YAMessageQueue();
		sendToMasterQueue = new YAMessageQueue();
		processMsgService = Executors.newFixedThreadPool(YAConfig.quorums);
	}
	
	public void connect(final String ip,final int port){
		
		Bootstrap bootstrap = new Bootstrap();
		
		
			bootstrap.group(loop)
			 .channel(NioSocketChannel.class)
			 .handler(new ChannelInitializer<SocketChannel>() {
				 @Override
				 public void initChannel(SocketChannel ch) throws Exception {
					 ChannelPipeline p = ch.pipeline();
					 p.addLast(
							//new LengthFieldPrepender(2),
						 	new ObjectEncoder(),
						 	//new LengthFieldBasedFrameDecoder(65535,0,2,0,2),
						 	new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
						 	new YAConfigClientHandler(YAConfig.client));
			 	 }
			 }).option(ChannelOption.SO_KEEPALIVE,true)
			   .option(ChannelOption.TCP_NODELAY,true);
			ChannelFuture f = bootstrap.connect(ip,port).awaitUninterruptibly()
			.addListener(new ConnectionListener(this,ip,port));		
	}

	@Override
	public void run() {
		for(Entry<String,EndPoint> e: YAConfig.getEps().eps.entrySet()){
			EndPoint ep = e.getValue();
			final String ip = ep.getIp();
			final int port = Integer.parseInt(ep.getPort());
			connectService.execute(new Runnable(){

				@Override
				public void run() {
					connect(ip,port);
				}
				
			});
		}
		
		processMsgService.execute(new Runnable(){
			@Override
			public void run(){
				while(true){
					try {
						YAMessage yamsg = rcvQueue.take();
						processMessage(yamsg);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
		
		sendToMasterThread = new Thread("sendToMasterThread"){
			@Override
			public void run(){
				while(true){
					
					YAMessage yamsg = null;
					try {
						
						yamsg = sendToMasterQueue.take();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					
					EndPoint currentMaster = YAConfig.getEps().currentMaster;
					if(currentMaster != null && yamsg != null){
						for(Entry<String,Channel> ep: channels.entrySet()){
							Channel channel = ep.getValue();
							if(channel.isActive()){
								InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
								if(address.getHostName().equals(currentMaster.getIp())
										&& currentMaster.getPort().equals(String.valueOf(address.getPort()))){
									try {
										System.out.println("send to master:" + yamsg.toString());
										channel.writeAndFlush(yamsg).sync();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
							}
						}
					}
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		sendToMasterThread.start();
	}

	public class ConnectionListener implements ChannelFutureListener {

		private YAConfigClient client;
		
		private String ip;
		
		private int port;
		
		public ConnectionListener(YAConfigClient client,String ip,int port){
			this.client = client;
			this.ip = ip;
			this.port = port;
		}
		
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if(!future.isSuccess()){
				future.channel().eventLoop().schedule(new Runnable(){

					@Override
					public void run() {
						YAConfig.printImportant("RECONNECT","reconnect " + ip + ":" + port);
						client.connect(ip,port);
					}
					
				}, 4, TimeUnit.SECONDS);
			}else{
				YAConfig.printImportant("CONNECT SUCCESS", "connect success " + ip + ":" + port);
			}
		}
		
	} 
	
	public void addChannel(Channel c){
		
		if(null == c || !c.isActive()){
			return;
		}
		
		InetSocketAddress address = (InetSocketAddress)c.remoteAddress();
		this.channels.putIfAbsent(
				address.getHostName() + ":" + String.valueOf(address.getPort()), c);
	}

	public void processMessage(YAMessage yamsg) {
		if(null == yamsg){
			return;
		}
		YAHashMap.getInstance().put(yamsg.key, yamsg.value);
		YAConfig.notifyWatchers(yamsg.key, yamsg.value);
	}

	public void redirectToMaster(YAMessage msg) {
		try {
			sendToMasterQueue.push(msg);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
}
