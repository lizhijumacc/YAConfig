package com.yaconfig.client.watchers;

import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.message.YAMessage;

public class RemoteWatchers extends AbstractWatchers {

	YAConfigClient client;
	
	public RemoteWatchers(){
		client = YAConfigClient.getInstance();
	}
	
	public void registerAllWatchers() {
		for(Watcher w : watchers.values()){
			client.writeCommand(w.getKey(), "".getBytes(), YAMessage.Type.WATCH);
		}
	}
	
	@Override
	public void watch(String key,WatcherListener... listeners){
		boolean needRegister = watchLocal(key, listeners);
		if(needRegister){
			client.writeCommand(key,"".getBytes(),YAMessage.Type.WATCH);
		}
	}
	
	@Override
	public void unwatch(String key,WatcherListener... listeners){
		boolean needUnregister = unwatchLocal(key,listeners);
		if(needUnregister){
			client.writeCommand(key,"".getBytes(),YAMessage.Type.UNWATCH);
		}
	}

}
