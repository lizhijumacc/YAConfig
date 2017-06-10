package com.yaconfig.server;

import java.util.ArrayList;

import io.netty.channel.ChannelId;

public class Watchers {
	private ArrayList<Watcher> watchers;
	
	public Watchers(){
		watchers = new ArrayList<Watcher>();
	}
	
	public void addWatcher(Watcher watcher){
		watchers.add(watcher);
	}

	public void notifyWatchers(String key,byte[] value,int type) {
		//TODO should use thread-safe container
		for(Watcher w : watchers){
			if(key.matches(w.getRexStr())){
				w.getChangeListener().onChange(key, value, type);
			}
		}
	}

	public void removeWatcher(ChannelId id) {
		for(Watcher w : watchers){
			if(w.getChannelId().equals(id)){
				watchers.remove(w);
			}
		}
	}
	
	public void removeWatcher(String key,ChannelId id){
		for(Watcher w : watchers){
			if(w.getChannelId().equals(id)
					&& w.getRexStr().equals(key)){
				watchers.remove(w);
			}
		}
	}
}
