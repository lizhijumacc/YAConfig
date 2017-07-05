package com.yaconfig.core.fetchers;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;

import com.yaconfig.client.YAConfigConnection;
import com.yaconfig.client.YAEntry;
import com.yaconfig.client.future.AbstractFuture;
import com.yaconfig.client.future.YAFuture;
import com.yaconfig.client.future.YAFutureListener;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.core.DataFrom;
import com.yaconfig.core.annotation.RemoteValue;

@FetcherType(from = DataFrom.REMOTE)
public class RemoteFetcher extends AbstractFetcher{
	
	public RemoteFetcher() {
		
	}

	@Override
	public void fetch(final Field field,final FetchCallback callback) {
		RemoteValue rv = field.getAnnotation(RemoteValue.class);
		String key = rv.key();
		String connStr = rv.connStr();
		
		final YAConfigConnection connection = new YAConfigConnection();
		connection.attach(connStr);
		YAFuture<YAEntry> f = connection.writeCommand(key, "".getBytes(), YAMessage.Type.GET_LOCAL);
		f.addListener(new YAFutureListener(key){
			
			@Override
			public void operationCompleted(AbstractFuture<YAEntry> future) {
				if(future.isSuccess()){
					try {
						callback.dataFetched(new String(future.get().getValue()),field,DataFrom.REMOTE);
						connection.detach();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
				} else {
					try {
						future.get();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
			
		});
	}
}
