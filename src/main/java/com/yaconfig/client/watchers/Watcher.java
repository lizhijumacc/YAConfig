package com.yaconfig.client.watchers;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

public class Watcher {
	private String key;
	private Collection<WatcherListener> listeners = new CopyOnWriteArrayList<WatcherListener>();
	
	public Watcher(String key){
		this.key = key;
	}
	
	public String getKey(){
		return key;
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
	
	public synchronized int listenerSize(){
		return listeners.size();
	}

	public synchronized void addListeners(WatcherListener[] watcherListeners) {
		for(WatcherListener wl : watcherListeners){
			addListener(wl);
		}
	}

	public void notifyListeners(YAEventType event,String key,int from) {
		for(WatcherListener wl : listeners){
			if(event == YAEventType.ADD){
				wl.onAdd(this,key,from);
			}else if(event == YAEventType.DELETE){
				wl.onDelete(this,key,from);
			}else if(event == YAEventType.UPDATE){
				wl.onUpdate(this,key,from);
			}
		}
	}
	
	
}
