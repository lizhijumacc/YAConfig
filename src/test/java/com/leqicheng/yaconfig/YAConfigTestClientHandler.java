package com.leqicheng.yaconfig;

import com.yaconfig.server.YAMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
		Channel channel = ctx.channel();
    	for(int i=0;i<5000;i++){
    		YAMessage yamsg = new YAMessage("com.test.test","qqqq".getBytes(),YAMessage.Type.PUT_NOPROMISE,i);
    		try {
				channel.writeAndFlush(yamsg).sync();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}	
    	
    	at.stop = true;
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx){
		System.out.println("write error!");
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// TODO Auto-generated method stub

	}

}
