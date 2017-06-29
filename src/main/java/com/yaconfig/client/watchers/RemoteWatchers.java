package com.yaconfig.client.watchers;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Set;

import com.yaconfig.client.Constants;
import com.yaconfig.client.YAConfigConnection;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.FieldChangeCallback;
import com.yaconfig.client.injector.FieldChangeListener;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.util.ConnStrKeyUtil;

public class RemoteWatchers extends AbstractWatchers {
	
	HashMap<String,YAConfigConnection> connections = new HashMap<String,YAConfigConnection>();
	
	public RemoteWatchers(Set<Field> fields,FieldChangeCallback callback){
		for(final Field field : fields){
			RemoteValue annotation = field.getAnnotation(RemoteValue.class);
			String key = annotation.key();
			String connStr = annotation.connStr();
			addConnection(connStr);
			Anchor anchor = field.getAnnotation(Anchor.class);
			if(anchor == null || anchor != null && anchor.anchor().equals(AnchorType.REMOTE)){
				watch(connStr + Constants.CONNECTION_KEY_SEPERATOR + key,new FieldChangeListener(field,callback));
			}
		}
	}
	
	private void addConnection(String connStr) {
		YAConfigConnection connection = new YAConfigConnection();
		connection.setRemoteWatchers(this);
		connection.attach(connStr);
		connections.put(connStr, connection);
	}

	public void registerAllWatchers(String connStr) {
		for(Watcher w : watchers.values()){
			if(w.getKey().startsWith(connStr)){
				String remoteKey = ConnStrKeyUtil.getKeyNameFromStr(w.getKey());
				connections.get(connStr).writeCommand(remoteKey, "".getBytes(), YAMessage.Type.WATCH);
			}
		}
	}
	
	@Override
	public void watch(String key,WatcherListener... listeners){
		boolean needRegister = watchLocal(key, listeners);
		if(needRegister){
			String remoteKey = ConnStrKeyUtil.getKeyNameFromStr(key);
			String connStr = ConnStrKeyUtil.getConnStrFromStr(key);
			YAConfigConnection c = connections.get(connStr);
			if(c != null){
				c.writeCommand(remoteKey,"".getBytes(),YAMessage.Type.WATCH);
			}
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
