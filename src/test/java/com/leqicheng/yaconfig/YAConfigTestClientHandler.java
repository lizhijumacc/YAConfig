package com.leqicheng.yaconfig;

import com.yaconfig.message.YAServerMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class YAConfigTestClientHandler extends ChannelInboundHandlerAdapter {

	AppTest at;
	
	public YAConfigTestClientHandler(AppTest test) {
		at = test;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){

	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx){
		
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// TODO Auto-generated method stub

	}

}
