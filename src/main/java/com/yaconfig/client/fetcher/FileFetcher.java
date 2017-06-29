package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;

import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.util.FileUtil;

public class FileFetcher extends AbstractFetcher{

	public FileFetcher(Field field) {
		super(field);
	}

	@Override
	public void fetch(String location, FetchCallback callback) {
		String data = FileUtil.readValueFromFile(location);
		callback.dataFetched(data, field, DataFrom.FILE);
	}

}
