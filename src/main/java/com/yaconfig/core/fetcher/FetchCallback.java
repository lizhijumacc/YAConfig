package com.yaconfig.core.fetcher;

import java.lang.reflect.Field;

import com.yaconfig.core.injector.DataFrom;

public interface FetchCallback {
	public void dataFetched(String data,Field field,int from);
}
