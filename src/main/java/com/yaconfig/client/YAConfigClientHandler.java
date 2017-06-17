package com.yaconfig.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class YAConfigClientHandler extends ChannelInboundHandlerAdapter {
	private YAConfigClient client;
	
	public YAConfigClientHandler(YAConfigClient client){
		this.client = client;
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx){
		client.channelActive(ctx.channel());
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		client.consume(msg);
		ReferenceCountUtil.release(msg);
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		client.exceptionCaught(ctx.channel(), cause);
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		client.channelInactive(ctx.channel());
	}
}
