package com.yaconfig.server;

public interface MessageConsumer {
	public abstract void consume(Object msg);
}
