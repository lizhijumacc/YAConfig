package com.yaconfig.core.syncs;

import java.lang.reflect.Field;

public interface Sync {
	public void sync(String data,Field field);
}
