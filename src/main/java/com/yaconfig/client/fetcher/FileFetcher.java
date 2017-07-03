package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;
import java.nio.file.Paths;

import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.util.ConnStrKeyUtil;
import com.yaconfig.client.util.FileUtil;

public class FileFetcher extends AbstractFetcher{

	public FileFetcher(Field field) {
		super(field);
	}

	@Override
	public void fetch(FetchCallback callback) {
		String path_r = field.getAnnotation(FileValue.class).path();
		String path = Paths.get(path_r).toAbsolutePath().toString();
		String key = field.getAnnotation(FileValue.class).key();
		String data = FileUtil.readValueFromFile(ConnStrKeyUtil.makeLocation(path, key));
		callback.dataFetched(data, field, DataFrom.FILE);
	}

}
