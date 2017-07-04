package com.yaconfig.client.watchers;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.reflections.Reflections;

import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.injector.FieldChangeCallback;

public abstract class AbstractWatchers implements Watchers{
	protected Map<String,Watcher> watchers;
	
	public AbstractWatchers(){
		watchers = new ConcurrentHashMap<String,Watcher>();
	}
	
	public boolean watchLocal(String key,WatcherListener... listeners){
		if(watchers.containsKey(key)){
			watchers.get(key).addListeners(listeners);
			return false;
		}else{
			Watcher watcher = new Watcher(key);
			watcher.addListeners(listeners);
			synchronized(this.watchers){
				watchers.put(key, watcher);
			}
			return true;
		}
	}

	public boolean unwatchLocal(String key,WatcherListener... listeners){
		if(!watchers.containsKey(key)){
			return false;
		}
		
		for(WatcherListener l : listeners){
			Watcher w = watchers.get(key);
			w.removeListener(l);
			if(w.listenerSize() == 0){
				watchers.remove(key);
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public abstract void watch(String key,WatcherListener...listeners);
	
	@Override
	public abstract void unwatch(String key,WatcherListener...listeners);
	
	@Override
	public abstract Set<Field> init(Reflections rfs,FieldChangeCallback callback);
	
	public void notifyWatchers(String key,YAEventType event,DataFrom from){
		for(Watcher w: watchers.values()){
			String wkey = w.getKey();
			if(key.matches(wkey) || wkey.equals(key)){
				w.notifyListeners(event,key,from);
			}
		}
	}
	
	public Watcher getWatcher(String key){
		return watchers.get(key);
	}
}
