package com.yaconfig.server;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;

public class YAConfigClientHandler extends ChannelInboundHandlerAdapter {

	private YAConfigClient client;
	
	public YAConfigClientHandler(YAConfigClient client){
		super();
		this.client = client;
	}
	
	
	@Override
	public void channelActive(ChannelHandlerContext ctx){
		client.addChannel(ctx.channel());
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		YAMessage yamsg = (YAMessage)msg;
		try {
			YAConfig.dumpPackage("client rcv a massge:",msg);
			if(yamsg.key.indexOf("com.yaconsfig") != 0){
				client.rcvQueue.push(yamsg);
				client.clientService.execute(client.clientRcvMessageTask);
			}/*else{
				client.systemRcvQueue.push(yamsg);
				client.clientService.execute(client.systemRcvMessageTask);
			}*/
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		reconnect(ctx);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		Channel channel = ctx.channel();
		client.peerDead(channel);
		client.removeChannel(channel);
		reconnect(ctx);
	}

	private void reconnect(ChannelHandlerContext ctx) throws Exception{
		final EventLoop eventLoop = ctx.channel().eventLoop();
		final InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
		
		eventLoop.schedule(new Runnable(){

			@Override
			public void run() {
				client.connect(address.getHostName(), address.getPort());
			}
			
		}, 1L, TimeUnit.SECONDS);	
	}
}
