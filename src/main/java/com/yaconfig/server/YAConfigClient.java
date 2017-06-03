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
import io.netty.handler.timeout.IdleStateHandler;

public class YAConfigClient implements Runnable{
	
	private EventLoopGroup loop = new NioEventLoopGroup();
	
	ExecutorService clientService;
	
	private ConcurrentHashMap<String,Channel> channels;
	
	YAMessageQueue rcvQueue;
	
	YAMessageQueue sendToMasterQueue;
	
	YAMessageQueue systemRcvQueue;
	
	Runnable sendToMasterTask;
	
	Runnable clientRcvMessageTask;
	
	private YAConfig yaconfig;

	public Runnable systemRcvMessageTask;

	public YAConfigClient(YAConfig yaconfig){
		loop = new NioEventLoopGroup();
		this.yaconfig = yaconfig;
		clientService = Executors.newCachedThreadPool();
		channels = new ConcurrentHashMap<String,Channel>();
		rcvQueue = new YAMessageQueue();
		sendToMasterQueue = new YAMessageQueue();
		systemRcvQueue = new YAMessageQueue();
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
						 new IdleStateHandler(4,0,0,TimeUnit.SECONDS),
						 new PeerDeadHandler(yaconfig),
						 new ObjectEncoder(),
						 //new LengthFieldBasedFrameDecoder(65535,0,2,0,2),
						 new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
						 new YAConfigClientHandler(yaconfig.client));
			 	}
			}).option(ChannelOption.SO_KEEPALIVE,true)
			  .option(ChannelOption.TCP_NODELAY,true);
		bootstrap.connect(ip,port).awaitUninterruptibly()
				.addListener(new ConnectionListener(this,ip,port));		
	}

	@Override
	public void run() {
		for(Entry<String,EndPoint> e: yaconfig.getEps().eps.entrySet()){
			EndPoint ep = e.getValue();
			final String ip = ep.getIp();
			final int port = Integer.parseInt(ep.getPort());
			clientService.execute(new Runnable(){

				@Override
				public void run() {
					connect(ip,port);
				}
				
			});
		}
		
		clientRcvMessageTask = new Runnable(){
			@Override
			public void run(){

					try {
						YAMessage yamsg = rcvQueue.take();
						processMessage(yamsg);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				
			}
		};
		
		systemRcvMessageTask = new Runnable(){
			@Override
			public void run(){

					try {
						YAMessage yamsg = systemRcvQueue.take();
						processMessage(yamsg);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				
			}
		};
		
		sendToMasterTask = new Runnable(){
			@Override
			public void run(){
					
					YAMessage yamsg = null;
					try {
						yamsg = sendToMasterQueue.take();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					
					EndPoint currentMaster = yaconfig.getEps().currentMaster;
					
					if(currentMaster != null && yamsg != null){
						for(Entry<String,Channel> ep: channels.entrySet()){
							Channel channel = ep.getValue();

							InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
							if(address.getHostName().equals(currentMaster.getIp())
									&& currentMaster.getPort().equals(String.valueOf(address.getPort()))){
								if(channel.isActive()){
									try {
										channel.writeAndFlush(yamsg).sync();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}			
								}
							}
						}
					}
				}
		};

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
						client.connect(ip,port);
					}
					
				}, 4, TimeUnit.SECONDS);
			}else{
				//YAConfig.printImportant("CONNECT SUCCESS", "connect success " + ip + ":" + port);
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

		if(yamsg.type == YAMessage.Type.PUT){
			if(yamsg.sequenceNum > YAConfig.VID){
				promise(yamsg);
			}else{
				System.out.println("yamsg.sequenceNum" + yamsg.sequenceNum);
				System.out.println("yaconfig.VID" + YAConfig.VID);
				nack(yamsg);
			}
		}else if(yamsg.type == YAMessage.Type.COMMIT){
			learn(yamsg);
		}else if(yamsg.type == YAMessage.Type.PUT_NOPROMISE){
			writeToStorage(yamsg);
		}
	}

	private void learn(YAMessage yamsg) {
		YAConfig.VID = yamsg.sequenceNum;
		writeToStorage(yamsg);	
	}

	private void writeToStorage(YAMessage yamsg) {
		YAHashMap.getInstance().put(yamsg.key, yamsg.value);
		yaconfig.notifyWatchers(yamsg.key, yamsg.value);
	}

	private void promise(YAMessage yamsg) {
		YAConfig.promisedNum = yamsg.sequenceNum;
		yamsg.type = YAMessage.Type.PROMISE;
		yamsg.serverID = YAConfig.SERVER_ID;
		try {
			sendToMasterQueue.push(yamsg);
			clientService.execute(sendToMasterTask);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void nack(YAMessage yamsg){
		//in muti-paxos no need to send nack message,
		//make sure there is only one master is enough
		System.out.println("SHOULD NOT BE HERE!");
		/*yamsg.serverID = YAConfig.SERVER_ID;
		yamsg.type = YAMessage.Type.NACK;
		try {
			sendToMasterQueue.push(yamsg);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
	}

	public void redirectToMaster(YAMessage msg) {
		try {
			sendToMasterQueue.push(msg);
			clientService.execute(sendToMasterTask);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}

	public void removeChannel(Channel channel) {
		InetSocketAddress address = (InetSocketAddress)channel.remoteAddress();
		channels.remove(address.getHostName() + ":" + String.valueOf(address.getPort()));
	}

	public void peerDead(Channel channel) {
		InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
		String ip = address.getHostName();
		String port = String.valueOf(address.getPort());
		yaconfig.peerDead(ip, port);
	}
}
