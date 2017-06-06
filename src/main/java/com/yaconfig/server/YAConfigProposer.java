package com.yaconfig.server;

import java.util.concurrent.TimeUnit;

import com.yaconfig.commands.PutCommand;
import com.yaconfig.message.UnPromisedMessages;
import com.yaconfig.message.YAServerMessage;
import com.yaconfig.message.YAServerMessageDecoder;
import com.yaconfig.message.YAServerMessageEncoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class YAConfigProposer extends MessageProcessor implements Runnable{
	
	private int port;
	
	Runnable bindTask;
	
	private UnPromisedMessages unPomisedMsgs;
	
	private YAConfig yaconfig;
	
	public YAConfigProposer(int port,YAConfig yaconfig){
		this.port = port;
		this.yaconfig = yaconfig;
		unPomisedMsgs = new UnPromisedMessages(this);
	}
	
	public void countPromise(YAServerMessage msg){
		this.unPomisedMsgs.countPromise(msg);
	}
	
	@Override
	public void run(){
		
		bindTask = new Runnable(){
			@Override
			public void run(){
				doBind(port);
			}
		};
		
		processService.execute(bindTask);
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
							new LengthFieldBasedFrameDecoder(65535,0,2,0,2),
							new YAServerMessageDecoder(),
							new IdleStateHandler(YAConfig.STATUS_REPORT_INTERVAL,0,0,TimeUnit.MILLISECONDS),
							new IdleHeartbeatHandler(yaconfig),
							new YAConfigMessageHandler(yaconfig.proposer),
							new LengthFieldPrepender(2),
							new YAServerMessageEncoder()
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
	
	public void broadcastToQuorums(YAServerMessage msg) {
		boardcast(msg);
	}

	@Override
	public void processMessageImpl(Object msg) {
		
		YAServerMessage yamsg = (YAServerMessage)msg;

		YAConfig.dumpPackage("Proposer rcv a massge:", yamsg);
		
		if(yamsg.getType() == YAServerMessage.Type.PUT){
			proposal(yamsg);
		}else if(yamsg.getType() == YAServerMessage.Type.PUT_NOPROMISE){
			PutCommand put = new PutCommand("put");
			put.setExecutor(yaconfig.exec);
			put.execute(yamsg.getKey(),yamsg.getValue(),true);			
		}else if(yamsg.getType() == YAServerMessage.Type.GET){
			//TODO
		}else if(yaconfig.statusEquals(EndPoint.Status.LEADING)
				&& yamsg.getType() == YAServerMessage.Type.PROMISE){
			countPromise(yamsg);
		}
	}

	private void proposal(YAServerMessage yamsg) {
		PutCommand put = new PutCommand("put");
		put.setExecutor(yaconfig.exec);
		put.execute(yamsg.getKey(),yamsg.getValue(),false);
	}

	public void setVID(String serverID, Long sequenceNum) {
		yaconfig.setVID(serverID,sequenceNum);
	}
	
	@Override
	public void addChannel(Channel channel){
		super.addChannel(channel);
		//new peer alive report myself immediately
		yaconfig.reportStatus();
	}
	
}
