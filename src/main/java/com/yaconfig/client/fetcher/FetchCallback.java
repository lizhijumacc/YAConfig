package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;

import com.yaconfig.client.injector.DataFrom;

public interface FetchCallback {
	public void dataFetched(String data,Field field,int from);
}
