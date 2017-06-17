package com.yaconfig.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;

public abstract class MessageProcessor extends ChannelContainer implements MessageProducer,MessageConsumer{
	protected ExecutorService processService;
	
	protected YAMessageQueue rcvQueue;
	
	protected YAMessageQueue boardcastQueue;
	
	public MessageProcessor(){
		processService = Executors.newCachedThreadPool();
		rcvQueue = new YAMessageQueue();
		boardcastQueue = new YAMessageQueue();
	}
	
	public void consume(Object msg){
		
		if(msg == null){
			return;
		}
		
		try {
			rcvQueue.push(msg);
			processService.execute(new Runnable(){

				@Override
				public void run() {
					try {
						processMessageImpl(rcvQueue.take());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
			});
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void produce(Object msg,final ChannelId id){
		if(id == null || msg == null){
			return;
		}
		
		Channel channel = channels.get(id);
		if(channel != null && channel.isWritable() && channel.isActive()){
			channel.writeAndFlush(msg);
		}
	}

	public void boardcast(Object msg){
		try {
			boardcastQueue.push(msg);
			
			processService.execute(new Runnable(){

				@Override
				public void run() {
					try {
						Object yamsg = boardcastQueue.take();
						
						for(Channel channel : channels.values()){
							if(channel.isWritable() && channel.isActive()){
								channel.writeAndFlush(yamsg);
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
			});
			
		} catch (InterruptedException e) {
		}
	}
	
	public abstract void processMessageImpl(Object msg);
	
}
