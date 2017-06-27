package com.yaconfig.test;

import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.message.YAMessageWrapper;
import com.yaconfig.server.message.YAServerMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class YAConfigTestClientHandler extends ChannelInboundHandlerAdapter {

	ServerTest at;
	
	public YAConfigTestClientHandler(ServerTest test) {
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
	public void channelRead(ChannelHandlerContext ctx,Object msg){
		YAMessageWrapper yamsg = (YAMessageWrapper)msg;
		System.out.println(yamsg.msg);
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
