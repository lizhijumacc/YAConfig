package com.yaconfig.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


public class YAConfigServerHandler extends ChannelInboundHandlerAdapter {

	private YAConfigServer server;
	
	public YAConfigServerHandler(YAConfigServer server){
		this.server = server;
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		YAMessage yamsg = (YAMessage)msg;
		System.out.println("server rcv a massge:" + yamsg.toString());
		server.processMessage(yamsg);
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){
		server.addChannel(ctx.channel());
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception{
		super.channelInactive(ctx);
		server.removeChannel(ctx.channel());
	}
	
}
