package com.yaconfig.server;

import com.yaconfig.message.YAMessage;
import com.yaconfig.message.YAMessageDecoder;
import com.yaconfig.message.YAMessageEncoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class YAConfigServer extends MessageProcessor implements Runnable{

	public YAConfigServer(){
		
	}
	
	@Override
	public void run() {
		Runnable clientServerBindTask = new Runnable(){
			@Override
			public void run(){
				doBind(8888);
			}
		};
		processService.execute(clientServerBindTask);
	}
	
	public void doBind(final int bindPort){

		ChannelFuture f;
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		
		final MessageProcessor self = this;
		
		try{
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup,workerGroup)
			 .channel(NioServerSocketChannel.class)
			 .childHandler(new ChannelInitializer<SocketChannel>(){

				@Override
				protected void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast(
							new LengthFieldBasedFrameDecoder(65535,0,2,0,2), 
							new YAMessageDecoder(),
							new YAConfigMessageHandler(self),
							new LengthFieldPrepender(2),
							new YAMessageEncoder()
						);
				}
				 
			 })
			 .option(ChannelOption.SO_BACKLOG, 128)
			 .option(ChannelOption.SO_REUSEADDR, true)
			 .childOption(ChannelOption.TCP_NODELAY, true)
			 .childOption(ChannelOption.SO_KEEPALIVE, true)
			 .childOption(ChannelOption.SO_REUSEADDR, true);
			
			f = b.bind(bindPort).sync().addListener(new ChannelFutureListener(){

				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if(!future.isSuccess()){
						System.out.println("start service error");
					}else{
						System.out.println("YAConfig service is started on port:" + bindPort);
					}
				}
				
			});
			f.channel().closeFuture().sync();
			
		} catch (InterruptedException e){
			e.printStackTrace();
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	
	}

	@Override
	public void processMessageImpl(Object msg) {
		YAMessage yamsg = (YAMessage)msg;
		if(yamsg.getType() == YAMessage.Type.PUT){
			System.out.println("putputputputputputput");
		}else if(yamsg.getType() == YAMessage.Type.PUT_NOPROMISE){
			System.out.println("putputputpunononotputputput");
		}else if(yamsg.getType() == YAMessage.Type.GET){
			
		}else if(yamsg.getType() == YAMessage.Type.GET_LOCAL){
			
		}else if(yamsg.getType() == YAMessage.Type.WATCH){
			
		}else if(yamsg.getType() == YAMessage.Type.UNWATCH){
			
		}
	}

}
