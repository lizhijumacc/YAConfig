package com.yaconfig.client.watchers;

import java.lang.reflect.Field;
import java.util.Set;

import org.reflections.Reflections;

import com.yaconfig.client.injector.FieldChangeCallback;

public interface Watchers {
	public void watch(String key,WatcherListener... listeners);
	public void unwatch(String key,WatcherListener... listeners);
	public Set<Field> init(Reflections rfs,FieldChangeCallback callback);
}
