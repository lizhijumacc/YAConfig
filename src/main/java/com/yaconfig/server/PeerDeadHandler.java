package com.yaconfig.server;

import java.net.InetSocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

public class PeerDeadHandler extends ChannelInboundHandlerAdapter {

	public YAConfig yaconfig;
	
	public PeerDeadHandler(YAConfig yaconfig){
		this.yaconfig = yaconfig;
	}
	
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if(evt instanceof IdleStateEvent){
			InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
			String ip = address.getHostName();
			String port = String.valueOf(address.getPort());
			System.out.println("idle!!!!!!!!!dead");
			yaconfig.peerDead(ip,port);
		}
		super.userEventTriggered(ctx, evt);
	}

}
