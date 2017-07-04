package com.yaconfig.core.syncs;

import java.lang.reflect.Field;

import com.yaconfig.core.annotation.FileValue;
import com.yaconfig.core.injector.DataFrom;
import com.yaconfig.core.util.ConnStrKeyUtil;
import com.yaconfig.core.util.FileUtil;

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
