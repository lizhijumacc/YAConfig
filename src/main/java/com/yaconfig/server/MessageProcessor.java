package com.yaconfig.server;

import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.yaconfig.message.YAMessageQueue;

import io.netty.channel.Channel;

public abstract class MessageProcessor extends ChannelContainer implements MessageProducer,MessageConsumer{
	ExecutorService processService;
	
	YAMessageQueue rcvQueue;
	
	YAMessageQueue sendQueue;
	
	YAMessageQueue boardcastQueue;
	
	public MessageProcessor(){
		processService = Executors.newCachedThreadPool();
		rcvQueue = new YAMessageQueue();
		sendQueue = new YAMessageQueue();
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
	
	public void produce(Object msg,final String host){
		
		if(host == null || msg == null){
			return;
		}
		
		try {
			sendQueue.push(msg);
			processService.execute(new Runnable(){

				@Override
				public void run() {
					try {
						Object sendMsg = sendQueue.take();
						
						for(Entry<String,Channel> ep: channels.entrySet()){
							Channel channel = ep.getValue();
							String key = ep.getKey();
							
							if(key.equals(host) && channel.isActive()){
								try {
									channel.writeAndFlush(sendMsg).sync();
									break;
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
			});
		} catch (InterruptedException e) {
			e.printStackTrace();
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
							if(channel.isActive()){
								channel.writeAndFlush(yamsg).sync();
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
