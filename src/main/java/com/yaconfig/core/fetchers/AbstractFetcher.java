package com.yaconfig.core.fetchers;

import java.lang.reflect.Field;

public abstract class AbstractFetcher implements Fetcher{
	
	public AbstractFetcher(){
	}
	
	@Override
	public abstract void fetch(Field field,FetchCallback callback);

}
