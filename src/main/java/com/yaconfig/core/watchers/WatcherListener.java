package com.yaconfig.core.watchers;

public interface WatcherListener {
	void onDelete(Watcher w, String key, int from);
	void onAdd(Watcher w, String key, int from);
	void onUpdate(Watcher w, String key, int from);
}
