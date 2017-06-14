package com.yaconfig.client;

public interface WatcherListener {
	void onDelete(Watcher w,String key);
	void onAdd(Watcher w,String key);
	void onUpdate(Watcher w,String key);
}
