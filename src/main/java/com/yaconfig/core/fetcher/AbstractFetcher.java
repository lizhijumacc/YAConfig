package com.yaconfig.core.fetcher;

import java.lang.reflect.Field;

public abstract class AbstractFetcher implements Fetcher{
	
	public AbstractFetcher(){
	}
	
	@Override
	public abstract void fetch(Field field,FetchCallback callback);

}
