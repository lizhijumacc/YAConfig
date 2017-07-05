package com.yaconfig.core.syncs;

import java.lang.reflect.Field;

import com.yaconfig.client.YAConfigConnection;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.core.DataFrom;
import com.yaconfig.core.annotation.RemoteValue;

@SyncType(from = DataFrom.REMOTE)
public class RemoteSync extends AbstractSync {

	@Override
	public void sync(String data,Field field) {
		final RemoteValue rv = field.getAnnotation(RemoteValue.class);
		if(rv != null){
			YAConfigConnection connection = new YAConfigConnection();
			connection.attach(rv.connStr());
			connection.put(rv.key(), data.getBytes(), YAMessage.Type.PUT_NOPROMISE).awaitUninterruptibly();
			connection.detach();
		}
	}

}
