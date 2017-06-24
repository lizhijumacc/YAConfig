package com.yaconfig.common;

import io.netty.channel.Channel;

public interface MessageConsumer {
	public abstract void consume(Channel channel,Object msg);
}
