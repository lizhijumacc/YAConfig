package com.yaconfig.core.syncs;

import java.lang.reflect.Field;

import com.yaconfig.client.YAConfigConnection;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.core.annotation.RemoteValue;
import com.yaconfig.core.injector.DataFrom;

@SyncType(from = DataFrom.REMOTE)
public class RemoteSync extends AbstractSync {

	@Override
	public void sync(String data,Field field,int from) {
		final RemoteValue rv = field.getAnnotation(RemoteValue.class);
		if(rv != null){
			YAConfigConnection connection = new YAConfigConnection();
			connection.attach(rv.connStr());
			connection.put(rv.key(), data.getBytes(), YAMessage.Type.PUT_NOPROMISE).awaitUninterruptibly();
			connection.detach();
		}
	}

}
