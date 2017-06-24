package com.yaconfig.server;

import java.net.InetSocketAddress;

import com.yaconfig.common.ChannelContainer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

public class PeerDeadHandler extends ChannelInboundHandlerAdapter {

	public YAConfig yaconfig;
	
	public ChannelContainer channels;
	
	public PeerDeadHandler(YAConfig yaconfig,ChannelContainer channels){
		this.yaconfig = yaconfig;
	}
	
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if(evt instanceof IdleStateEvent){
			InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
			String ip = address.getHostName();
			String port = String.valueOf(address.getPort());
			yaconfig.peerDead(ip,port);
			channels.removeChannel(ctx.channel());
		}
		super.userEventTriggered(ctx, evt);
	}

}
