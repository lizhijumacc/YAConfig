package com.yaconfig.server;

import java.util.HashSet;
import java.util.Set;

import io.netty.channel.ChannelId;

public class ServerWatchers {
	private Set<ServerWatcher> watchers;
	
	public ServerWatchers(){
		watchers = new HashSet<ServerWatcher>();
	}
	
	public synchronized void addWatcher(ServerWatcher watcher){
		if(!watchers.contains(watcher)){
			watchers.add(watcher);
		}
	}

	public synchronized void notifyWatchers(String key,byte[] value,int type) {
		//TODO should use thread-safe container
		for(ServerWatcher w : watchers){
			if(key.matches(w.getRexStr())){
				w.getChangeListener().onChange(key, value, type);
			}
		}
	}

	public synchronized void removeWatcher(ChannelId id) {
		for(ServerWatcher w : watchers){
			if(w.getChannelId().equals(id)){
				watchers.remove(w);
			}
		}
	}
	
	public synchronized void removeWatcher(String key,ChannelId id){
		ServerWatcher w = new ServerWatcher(key,id);
		watchers.remove(w);
	}
}
