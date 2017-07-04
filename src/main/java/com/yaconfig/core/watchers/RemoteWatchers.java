package com.yaconfig.core.watchers;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Set;

import org.reflections.Reflections;

import com.yaconfig.core.Constants;
import com.yaconfig.client.YAConfigConnection;
import com.yaconfig.client.YAEntry;
import com.yaconfig.client.future.AbstractFuture;
import com.yaconfig.client.future.FutureListener;
import com.yaconfig.client.future.YAFuture;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.core.annotation.Anchor;
import com.yaconfig.core.annotation.RemoteValue;
import com.yaconfig.core.injector.AnchorType;
import com.yaconfig.core.injector.DataFrom;
import com.yaconfig.core.injector.FieldChangeCallback;
import com.yaconfig.core.injector.FieldChangeListener;
import com.yaconfig.core.util.ConnStrKeyUtil;

@WatchersType(from = DataFrom.REMOTE)
public class RemoteWatchers extends AbstractWatchers {
	
	HashMap<String,YAConfigConnection> connections = new HashMap<String,YAConfigConnection>();
	
	public RemoteWatchers(){
		
	}
	
	public RemoteWatchers(Reflections rfs,FieldChangeCallback callback){
		this.init(rfs, callback);
	}
	
	@Override
	public Set<Field> init(Reflections rfs,FieldChangeCallback callback){
		Set<Field> fields = rfs.getFieldsAnnotatedWith(RemoteValue.class);
		for(final Field field : fields){
			RemoteValue annotation = field.getAnnotation(RemoteValue.class);
			String key = annotation.key();
			String connStr = annotation.connStr();
			Anchor anchor = field.getAnnotation(Anchor.class);
			if(anchor == null || anchor != null && anchor.anchor() == AnchorType.REMOTE){
				watch(connStr + Constants.CONNECTION_KEY_SEPERATOR + key,new FieldChangeListener(field,callback));
			}
		}
		
		return fields;
	}

	public void registerAllWatchers(String connStr) {
		for(Watcher w : watchers.values()){
			if(w.getKey().startsWith(connStr)){
				String remoteKey = ConnStrKeyUtil.getKeyNameFromStr(w.getKey());
				watchRemote(remoteKey,connStr);
			}
		}
	}
	
	@Override
	public void watch(final String key,final WatcherListener... listeners){
		boolean needRegister = watchLocal(key, listeners);
		if(needRegister){
			String remoteKey = ConnStrKeyUtil.getKeyNameFromStr(key);
			String connStr = ConnStrKeyUtil.getConnStrFromStr(key);
			watchRemote(remoteKey,connStr);
		}
	}
	
	private void watchRemote(final String remoteKey, final String connStr) {
		YAConfigConnection c = connections.get(connStr);
		if(c == null){
			addConnection(connStr);
		}
		//TODO: should add retry policy.
		YAFuture<YAEntry> f = connections.get(connStr).writeCommand(remoteKey,"".getBytes(),YAMessage.Type.WATCH);
		f.addListener(new FutureListener<YAEntry>(){

			@Override
			public void operationCompleted(AbstractFuture<YAEntry> abstractFuture) {
				if(!abstractFuture.isSuccess()){
					watchRemote(remoteKey,connStr);
				}
			}
			
		});
	}

	private void addConnection(String connStr) {
		if(!connections.containsKey(connStr)){
			YAConfigConnection connection = new YAConfigConnection();
			connection.setRemoteWatchers(this);
			connection.attach(connStr);
			connections.put(connStr, connection);
		}
	}
	
	@Override
	public void unwatch(String key,WatcherListener... listeners){
		boolean needUnregister = unwatchLocal(key,listeners);
		if(needUnregister){
			String remoteKey = ConnStrKeyUtil.getKeyNameFromStr(key);
			String connStr = ConnStrKeyUtil.getConnStrFromStr(key);
			connections.get(connStr).writeCommand(remoteKey,"".getBytes(),YAMessage.Type.UNWATCH);
		}
	}

}
