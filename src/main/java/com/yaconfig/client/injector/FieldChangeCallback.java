package com.yaconfig.client.injector;

import java.lang.reflect.Field;

public interface FieldChangeCallback {
	void onAdd(String key,Field field,DataFrom from);
	void onUpdate(String key,Field field,DataFrom from);
	void onDelete(String key,Field field,DataFrom from);
}
