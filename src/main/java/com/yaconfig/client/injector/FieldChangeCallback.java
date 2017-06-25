package com.yaconfig.client.injector;

import java.lang.reflect.Field;

public interface FieldChangeCallback {
	void onAdd(String key,Field field);
	void onUpdate(String key,Field field);
	void onDelete(String key,Field field);
}
