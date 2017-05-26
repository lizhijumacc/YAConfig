package com.yaconfig.server;

import java.util.ArrayList;

public class Watchers {
	private ArrayList<Watcher> watchers;
	
	public Watchers(){
		watchers = new ArrayList<Watcher>();
	}
	
	public void addWatcher(Watcher watcher){
		watchers.add(watcher);
	}

	public void notifyWatchers(String key,byte[] value) {
		for(Watcher w : watchers){
			if(key.matches(w.getRexStr())){
				w.getChangeListener().onChange(key, value);
			}
		}
	}
}
