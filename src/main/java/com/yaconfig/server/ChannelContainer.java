package com.yaconfig.server;

import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;

public abstract class ChannelContainer {
	ConcurrentHashMap<ChannelId,Channel> channels;
	
	public ChannelContainer(){
		channels = new ConcurrentHashMap<ChannelId,Channel>();
	}
	
	public void addChannel(Channel c){
		if(null == c || !c.isActive()){
			return;
		}

		this.channels.putIfAbsent(c.id(), c);
	}
	
	public Channel getChannel(ChannelId id){
		if(null != id){
			return channels.get(id);
		}
		return null;
	}
	
	public void removeChannel(Channel channel) {
		channels.remove(channel.id());
		channel.close();
	}
	
	public void channelActive(Channel channel){
		addChannel(channel);
	}
	
	public void channelInactive(Channel channel){
		removeChannel(channel);
	}
	
	public void exceptionCaught(Channel channel, Throwable cause) throws Exception {
		removeChannel(channel);
	}
	
}
