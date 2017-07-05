package com.yaconfig.core.fetchers;

import java.lang.reflect.Field;
import java.nio.file.Paths;

import com.yaconfig.core.DataFrom;
import com.yaconfig.core.annotation.FileValue;
import com.yaconfig.core.util.ConnStrKeyUtil;
import com.yaconfig.core.util.FileUtil;

@FetcherType(from = DataFrom.FILE)
public class FileFetcher extends AbstractFetcher{

	public FileFetcher() {

	}

	@Override
	public void fetch(Field field,FetchCallback callback) {
		String path_r = field.getAnnotation(FileValue.class).path();
		String path = Paths.get(path_r).toAbsolutePath().toString();
		String key = field.getAnnotation(FileValue.class).key();
		String data = FileUtil.readValueFromFile(ConnStrKeyUtil.makeLocation(path, key));
		callback.dataFetched(data, field, DataFrom.FILE);
	}

}
