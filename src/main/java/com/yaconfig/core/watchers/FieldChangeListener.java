package com.yaconfig.core.watchers;

import java.lang.reflect.Field;

public class FieldChangeListener implements WatcherListener{
	
	private Field watchField;
	private FieldChangeCallback callback;
	
	public FieldChangeListener(Field watchField,FieldChangeCallback callback){
		this.watchField = watchField;
		this.callback = callback;
	}
	
	public void onDelete(Watcher w, String key, int from) {
		callback.onDelete(key,watchField,from);
	}

	public void onAdd(Watcher w, String key, int from) {
		callback.onAdd(key,watchField,from);
	}

	public void onUpdate(Watcher w, String key, int from) {
		callback.onUpdate(key,watchField,from);
	}
	
	public Field getField(){
		return this.watchField;
	}
}