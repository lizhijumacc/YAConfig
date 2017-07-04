package com.yaconfig.client.syncs;

import java.lang.reflect.Field;

public interface Sync {
	public void sync(String data,Field field,int from);
}
