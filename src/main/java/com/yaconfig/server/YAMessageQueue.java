package com.yaconfig.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class YAMessageQueue {
	BlockingQueue<YAMessage> queue;
	
	public YAMessageQueue(){
		queue = new LinkedBlockingQueue<YAMessage>();
	}
	
	public void push(YAMessage msg) throws InterruptedException{
		queue.put(msg);
	}
	
	public YAMessage take() throws InterruptedException{
		return queue.take();
	}
	
	public int size(){
		return queue.size();
	}
}
