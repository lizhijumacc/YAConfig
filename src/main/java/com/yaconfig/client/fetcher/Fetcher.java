package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;

public interface Fetcher {
	public void fetch(Field field,FetchCallback callback);
}
