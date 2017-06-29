package com.yaconfig.client.watchers;

import com.yaconfig.client.injector.DataFrom;

public interface WatcherListener {
	void onDelete(Watcher w, String key, DataFrom from);
	void onAdd(Watcher w, String key, DataFrom from);
	void onUpdate(Watcher w, String key, DataFrom from);
}
