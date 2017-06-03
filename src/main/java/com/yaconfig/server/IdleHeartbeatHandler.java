package com.yaconfig.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;

public class IdleHeartbeatHandler extends ChannelInboundHandlerAdapter {

	private YAConfig yaconfig;
	
	public IdleHeartbeatHandler(YAConfig yaconfig){
		this.yaconfig = yaconfig;
	}
	
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if(evt instanceof IdleStateEvent){
			yaconfig.reportStatus();
		}
		super.userEventTriggered(ctx, evt);
	}

}
