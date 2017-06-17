package com.yaconfig.server;

import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.message.YAMessageDecoder;
import com.yaconfig.client.message.YAMessageEncoder;
import com.yaconfig.client.message.YAMessageWrapper;
import com.yaconfig.server.commands.PutCommand;
import com.yaconfig.server.storage.YAHashMap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class YAConfigServer extends MessageProcessor implements Runnable{

	YAConfig yaconfig;
	
	public YAConfigServer(YAConfig yaconfig){
		this.yaconfig = yaconfig;
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
			 .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
			 .childOption(ChannelOption.TCP_NODELAY, true)
			 .childOption(ChannelOption.SO_KEEPALIVE, true)
			 .childOption(ChannelOption.SO_REUSEADDR, true)
			 .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
			
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
		YAMessageWrapper yamsgw = (YAMessageWrapper)msg;
		ChannelHandlerContext ctx = yamsgw.ctx;
		YAMessage yamsg = yamsgw.msg;

		if(yamsg.getType() == YAMessage.Type.PUT){
			PutCommand put = new PutCommand("put");
			put.setExecutor(yaconfig.exec);
			put.execute(yamsg.getKey(), yamsg.getValue(), false);
			ack(yamsgw,"".getBytes());
		}else if(yamsg.getType() == YAMessage.Type.PUT_NOPROMISE){
			PutCommand put = new PutCommand("put");
			put.setExecutor(yaconfig.exec);
			put.execute(yamsg.getKey(), yamsg.getValue(), true);
			ack(yamsgw,"".getBytes());
		}else if(yamsg.getType() == YAMessage.Type.GET){
			//TODO : should GET from current master
			ack(yamsgw,YAHashMap.getInstance().get(yamsg.key));
		}else if(yamsg.getType() == YAMessage.Type.GET_LOCAL){
			ack(yamsgw,YAHashMap.getInstance().get(yamsg.key));
		}else if(yamsg.getType() == YAMessage.Type.WATCH){
			final ServerWatcher watcher = new ServerWatcher(yamsg.key);
			watcher.setChannelId(ctx.channel().id());
			
			watcher.setChangeListener(new IChangeListener(){

				@Override
				public void onChange(String key,byte[] value,int type) {
					
					Channel channel = yaconfig.server.getChannel(watcher.getChannelId());
					
					if(channel != null && channel.isActive()){
						YAMessage yamsg = new YAMessage(type,key,value);
						yaconfig.server.produce(yamsg, channel.id());
					}
				}
				
			});
			
			yaconfig.getWatcherSet().addWatcher(watcher);
			ack(yamsgw,"".getBytes());
		}else if(yamsg.getType() == YAMessage.Type.UNWATCH){
			yaconfig.getWatcherSet().removeWatcher(yamsg.key,ctx.channel().id());
			ack(yamsgw,"".getBytes());
		}
	}
	
	private void ack(YAMessageWrapper yamsgw, byte[] bytes) {
		YAMessage sendMsg = new YAMessage(YAMessage.Type.ACK,yamsgw.msg.getKey(),bytes);
		sendMsg.setId(yamsgw.msg.getId());
		produce(sendMsg,yamsgw.ctx.channel().id());
	}

	@Override
	public void channelInactive(Channel channel){
		yaconfig.getWatcherSet().removeWatcher(channel.id());
		super.channelInactive(channel);
	}

}
