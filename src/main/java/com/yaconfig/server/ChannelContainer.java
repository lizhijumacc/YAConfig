package com.yaconfig.server;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.Channel;

public abstract class ChannelContainer {
	ConcurrentHashMap<String,Channel> channels;
	
	public ChannelContainer(){
		channels = new ConcurrentHashMap<String,Channel>();
	}
	
	public void addChannel(Channel c){
		if(null == c || !c.isActive()){
			return;
		}
		InetSocketAddress address = (InetSocketAddress)c.remoteAddress();
		this.channels.putIfAbsent(
				address.getHostName()+ ":" + String.valueOf(address.getPort()), c);
	}
	
	public void removeChannel(Channel channel) {
		InetSocketAddress address = (InetSocketAddress)channel.remoteAddress();
		channels.remove(address.getHostName()+ ":" + String.valueOf(address.getPort()));
	}
	
	public void channelInactive(Channel channel){
		removeChannel(channel);
	}
	
	public void exceptionCaught(Channel channel, Throwable cause) throws Exception {
		removeChannel(channel);
	}
}
