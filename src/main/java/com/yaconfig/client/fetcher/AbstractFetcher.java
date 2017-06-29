package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;

public abstract class AbstractFetcher implements Fetcher{

	Field field;
	
	public AbstractFetcher(Field field){
		this.field = field;
	}
	
	@Override
	public abstract void fetch(String location, FetchCallback callback);

}
