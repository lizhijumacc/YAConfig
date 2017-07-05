package com.yaconfig.core.fetchers;

import java.lang.reflect.Field;

import com.yaconfig.core.DataFrom;

public interface FetchCallback {
	public void dataFetched(String data,Field field,int from);
}
