package com.yaconfig.client;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import com.yaconfig.client.message.YAMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class Watcher {
	private String key;
	private Channel channel;
	private Collection<WatcherListener> listeners = new CopyOnWriteArrayList<WatcherListener>();
	
	public Watcher(String key,Channel channel){
		this.key = key;
		this.channel = channel;
	}
	
	public String getKey(){
		return key;
	}
	
	public Watcher watch() throws Exception{
		if(channel.isWritable()){
			YAMessage msg = new YAMessage(YAMessage.Type.WATCH,key,"".getBytes());
			ChannelFuture cf = channel.writeAndFlush(msg).syncUninterruptibly();
			if(cf.isSuccess()){
				return this;
			}else{
				throw new Exception("unsuccessfully watch");
			}
		}
		
		throw new Exception("unsuccessfully watch");
	}
	
	public Channel channel(){
		return this.channel;
	}
 
	public synchronized void addListener(WatcherListener watcherListener) {
		listeners.add(watcherListener);
	}
	
	public synchronized void removeListener(WatcherListener watcherListener){
		listeners.remove(watcherListener);
	}

	public synchronized void addListeners(Collection<? extends WatcherListener> watcherListeners) {
		listeners.addAll(watcherListeners);
	}

	public synchronized void addListeners(WatcherListener[] watcherListeners) {
		for(WatcherListener wl : watcherListeners){
			addListener(wl);
		}
	}

	public void notifyListeners(int event,String key) {
		for(WatcherListener wl : listeners){
			if(event == YAMessage.Type.ADD){
				wl.onAdd(this,key);
			}else if(event == YAMessage.Type.DELETE){
				wl.onDelete(this,key);
			}else if(event == YAMessage.Type.UPDATE){
				wl.onUpdate(this,key);
			}
		}
	}
	
	
}
