package com.yaconfig.server;

public interface MessageProducer {
	public abstract void produce(Object msg,String host);
	
	public abstract void boardcast(Object msg);
}
