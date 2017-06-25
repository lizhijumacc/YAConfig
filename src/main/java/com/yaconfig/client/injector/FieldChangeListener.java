package com.yaconfig.client.injector;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;

import com.yaconfig.client.Watcher;
import com.yaconfig.client.WatcherListener;
import com.yaconfig.client.message.YAMessage;

public class FieldChangeListener implements WatcherListener{
	
	private Field watchField;
	private FieldChangeCallback callback;
	
	public FieldChangeListener(Field watchField,FieldChangeCallback callback){
		this.watchField = watchField;
		this.callback = callback;
	}
	
	public void onDelete(Watcher w, String key) {
		callback.onDelete(key,watchField);
	}

	public void onAdd(Watcher w, String key) {
		callback.onAdd(key,watchField);
	}

	public void onUpdate(Watcher w, String key) {
		callback.onUpdate(key,watchField);
	}
}