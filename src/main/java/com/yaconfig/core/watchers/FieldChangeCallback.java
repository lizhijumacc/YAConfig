package com.yaconfig.core.watchers;

import java.lang.reflect.Field;

public interface FieldChangeCallback {
	void onAdd(String key,Field field,int from);
	void onUpdate(String key,Field field,int from);
	void onDelete(String key,Field field,int from);
}
