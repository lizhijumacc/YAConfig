package com.yaconfig.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class YAConfigMessageHandler extends ChannelInboundHandlerAdapter {

	private MessageProcessor processor;
	
	public YAConfigMessageHandler(MessageProcessor processor){
		this.processor = processor;
	}
	
	
	@Override
	public void channelActive(ChannelHandlerContext ctx){
		processor.addChannel(ctx.channel());
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		processor.consume(msg);
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		processor.exceptionCaught(ctx.channel(),cause);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		processor.channelInactive(ctx.channel());
	}
}
