package com.yaconfig.client.syncs;

import java.lang.reflect.Field;

import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.util.ConnStrKeyUtil;
import com.yaconfig.client.util.FileUtil;

@SyncType(from = DataFrom.FILE)
public class FileSync extends AbstractSync {

	@Override
	public void sync(String data,Field field,int from) {
		final FileValue fv = field.getAnnotation(FileValue.class);
		if(fv != null){
			FileUtil.writeValueToFile(ConnStrKeyUtil.makeLocation(fv.path(), fv.key()), data);
		}
	}

}
