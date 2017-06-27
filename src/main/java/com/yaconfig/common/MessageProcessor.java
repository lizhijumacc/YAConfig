package com.yaconfig.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

public abstract class MessageProcessor extends ChannelContainer implements MessageProducer,MessageConsumer{
	protected ThreadPoolExecutor processService;
	
	private static final int DEFAULT_LOW_WATER_MARK = 1024;
	private static final int DEFAULT_HIGH_WATER_MARK = 2048;
	
	private int lowWaterMark;
	private int highWaterMark;
	
	public MessageProcessor(){
		processService = (ThreadPoolExecutor)Executors.newCachedThreadPool();
		lowWaterMark = DEFAULT_LOW_WATER_MARK;
		highWaterMark = DEFAULT_HIGH_WATER_MARK;
	}
	
	public MessageProcessor(ThreadPoolExecutor executor){
		processService = executor;
		lowWaterMark = DEFAULT_LOW_WATER_MARK;
		highWaterMark = DEFAULT_HIGH_WATER_MARK;
	}
	
	public MessageProcessor(ThreadPoolExecutor executor,int low,int high){
		processService = executor;
		lowWaterMark = low;
		highWaterMark = high;
	}
	
	public void consume(Channel channel,final Object msg){
		if(msg == null){
			return;
		}

		processService.execute(new Runnable(){

			@Override
			public void run() {
				processMessageImpl(msg);
			}
			
		});
		
		int queued = processService.getQueue().size();
		if(queued > highWaterMark){ 
			channel.config().setAutoRead(false); 
		}if(queued < lowWaterMark){
			channel.config().setAutoRead(true); 
		}
		
		ReferenceCountUtil.release(msg);
	}
	
	public void produce(Object msg,final ChannelId id){
		if(id == null || msg == null){
			return;
		}

		while(!checkChannel(channels.get(id))){
			try {
				if(channels.get(id) != null){
					channels.get(id).wait();
				}else{
					//channel is already been removed, just discard the message.
					return;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		channels.get(id).writeAndFlush(msg);
	}

	public void boardcast(Object msg){
		for(ChannelId cid : channels.keySet()){
			produce(msg,cid);
		}
	}
	
	public boolean checkChannel(Channel channel){
		return channel != null && channel.isWritable() && channel.isActive();
	}

	public void channelWritabilityChanged(Channel c) {
		Channel channel = channels.get(c.id());
		synchronized(channel){
			if(checkChannel(channel)){
				channel.notifyAll();
			}
		}
	}
	
	@Override
	public void channelActive(Channel channel){
		super.channelActive(channel);
		//wait until channel is writable
		synchronized(channel){
			while(!channel.isWritable()){}
			channel.notifyAll();
		}
	}
	
	public abstract void processMessageImpl(Object msg);
	
}
