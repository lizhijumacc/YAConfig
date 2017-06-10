package com.yaconfig.server;

import java.util.HashSet;
import java.util.Set;

import io.netty.channel.ChannelId;

public class Watchers {
	private Set<Watcher> watchers;
	
	public Watchers(){
		watchers = new HashSet<Watcher>();
	}
	
	public synchronized void addWatcher(Watcher watcher){
		if(!watchers.contains(watcher)){
			watchers.add(watcher);
		}
	}

	public synchronized void notifyWatchers(String key,byte[] value,int type) {
		//TODO should use thread-safe container
		for(Watcher w : watchers){
			if(key.matches(w.getRexStr())){
				w.getChangeListener().onChange(key, value, type);
			}
		}
	}

	public synchronized void removeWatcher(ChannelId id) {
		for(Watcher w : watchers){
			if(w.getChannelId().equals(id)){
				watchers.remove(w);
			}
		}
	}
	
	public synchronized void removeWatcher(String key,ChannelId id){
		Watcher w = new Watcher(key,id);
		watchers.remove(w);
	}
}
