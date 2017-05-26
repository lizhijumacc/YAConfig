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
		System.out.println(msg.toString());
		ctx.write(msg);
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){
		System.out.println("server active!");
		server.addChannel(ctx.channel());
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception{
		super.channelInactive(ctx);
		System.out.println("server inactive!");
		server.removeChannel(ctx.channel());
	}
	
}
