package com.yaconfig.server;

import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.yaconfig.message.YAServerMessage;
import com.yaconfig.message.YAServerMessageDecoder;
import com.yaconfig.message.YAServerMessageEncoder;
import com.yaconfig.storage.YAHashMap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;

public class YAConfigAcceptor extends MessageProcessor implements Runnable{
	
	private EventLoopGroup loop;
	
	private YAConfig yaconfig;

	public YAConfigAcceptor(YAConfig yaconfig){
		loop = new NioEventLoopGroup();
		this.yaconfig = yaconfig;
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
						 new LengthFieldBasedFrameDecoder(65535,0,2,0,2),
						 new YAServerMessageDecoder(),
						 new IdleStateHandler(2 * YAConfig.STATUS_REPORT_INTERVAL,0,0,TimeUnit.MILLISECONDS),
						 new PeerDeadHandler(yaconfig),
						 new YAConfigMessageHandler(yaconfig.acceptor),
						 new LengthFieldPrepender(2),
						 new YAServerMessageEncoder()
						);
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
			processService.execute(new Runnable(){

				@Override
				public void run() {
					connect(ip,port);
				}
				
			});
		}

	}

	public class ConnectionListener implements ChannelFutureListener {

		private YAConfigAcceptor client;
		
		private String ip;
		
		private int port;
		
		public ConnectionListener(YAConfigAcceptor client,String ip,int port){
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
				YAConfig.printImportant("CONNECT SUCCESS", "connect success " + ip + ":" + port);
			}
		}
		
	} 
	
	@Override
	public void processMessageImpl(Object msg){
		
		YAServerMessage yamsg = (YAServerMessage)msg;
		
		if(yamsg.getType() == YAServerMessage.Type.PUT){
			if(yamsg.getSequenceNum() > YAConfig.VID.get()){
				promise(yamsg);
			}else{
				System.out.println("yamsg.sequenceNum" + yamsg.getSequenceNum());
				System.out.println("yaconfig.VID" + YAConfig.VID);
				nack(yamsg);
			}
		}else if(yamsg.getType() == YAServerMessage.Type.COMMIT){
			learn(yamsg);
		}else if(yamsg.getType() == YAServerMessage.Type.PUT_NOPROMISE){
			writeToStorage(yamsg);
		}
	}

	private void learn(YAServerMessage yamsg) {
		YAConfig.VID.set(yamsg.getSequenceNum());
		writeToStorage(yamsg);	
	}

	private void writeToStorage(YAServerMessage yamsg) {
		YAHashMap.getInstance().put(yamsg.getKey(), yamsg.getValue());
		yaconfig.notifyWatchers(yamsg.getKey(), yamsg.getValue());
	}

	private void promise(YAServerMessage yamsg) {
		YAConfig.promisedNum.set(yamsg.getSequenceNum());
		YAServerMessage sendMsg = new YAServerMessage(yamsg.getKey(),"".getBytes(),
				YAServerMessage.Type.PROMISE,yamsg.getSequenceNum());
		produce(sendMsg,yaconfig.getEps().currentMaster.host());
	}
	
	private void nack(YAServerMessage yamsg){
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

	public void redirectToMaster(YAServerMessage msg) {
		produce(msg,yaconfig.getEps().currentMaster.host());
	}

	public void peerDead(Channel channel) {
		InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
		String ip = address.getHostName();
		String port = String.valueOf(address.getPort());
		yaconfig.peerDead(ip, port);
	}
	
	@Override
	public void channelInactive(Channel channel){
		peerDead(channel);
		super.removeChannel(channel);
		try {
			reconnect(channel);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void exceptionCaught(Channel channel, Throwable cause) throws Exception {
		try {
			reconnect(channel);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void reconnect(Channel channel) throws Exception{
		final EventLoop eventLoop = channel.eventLoop();
		final InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
		
		eventLoop.schedule(new Runnable(){

			@Override
			public void run() {
				connect(address.getHostName(), address.getPort());
			}
			
		}, 1L, TimeUnit.SECONDS);	
	}

}
