package com.yaconfig.server;

import io.netty.channel.ChannelId;

public interface MessageProducer {
	public abstract void produce(Object msg,ChannelId id);
	
	public abstract void boardcast(Object msg);
}
