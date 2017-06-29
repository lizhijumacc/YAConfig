package com.yaconfig.client.watchers;

public interface Watchers {
	public void watch(String key,WatcherListener... listeners);
	public void unwatch(String key,WatcherListener... listeners);
}
