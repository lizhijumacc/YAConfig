package com.yaconfig.message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class YAMessageQueue {
	BlockingQueue<Object> queue;
	
	public YAMessageQueue(){
		queue = new LinkedBlockingQueue<Object>();
	}
	
	public void push(Object msg) throws InterruptedException{
		queue.put(msg);
	}
	
	public Object take() throws InterruptedException{
		return queue.take();
	}
	
	public int size(){
		return queue.size();
	}
}
