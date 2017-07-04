package com.yaconfig.client.watchers;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.reflections.Reflections;

import com.yaconfig.client.Constants;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.MySQLValue;
import com.yaconfig.client.annotation.RedisValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.injector.FieldChangeCallback;
import com.yaconfig.client.injector.FieldChangeListener;
import com.yaconfig.client.util.ConnStrKeyUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import com.yaconfig.client.Constants;

@WatchersType(from = Constants.fromRedis)
public class RedisWatchers extends AbstractWatchers {

	Map<String,Jedis> connections = new ConcurrentHashMap<String,Jedis>();
	Map<Jedis,ExecutorService> watchTask = new ConcurrentHashMap<Jedis,ExecutorService>();
	RedisWatchers myself;
	
	public RedisWatchers(){
		
	}
	
	public RedisWatchers(Reflections rfs, FieldChangeCallback callback) {
		this.init(rfs, callback);
	}
	
	@Override
	public Set<Field> init(Reflections rfs, FieldChangeCallback callback) {
		myself = this;
		Set<Field> fields = rfs.getFieldsAnnotatedWith(RedisValue.class);
		for(final Field field : fields){
			RedisValue annotation = field.getAnnotation(RedisValue.class);
			String key = annotation.key();
			String connStr = annotation.connStr();
			Anchor anchor = field.getAnnotation(Anchor.class);
			if(anchor == null || anchor != null && anchor.anchor().equals(AnchorType.REDIS)){
				watch(connStr + Constants.CONNECTION_KEY_SEPERATOR + key,new FieldChangeListener(field,callback));
			}
		}
		
		return fields;
	}

	@Override
	public void watch(String key, WatcherListener... listeners) {
		boolean needRegister = watchLocal(key, listeners);
		if(needRegister){
			String connStr = ConnStrKeyUtil.getConnStrFromStr(key);
			addConnection(connStr);			
		}
	}

	private Jedis addConnection(final String connStr) {
		if(connections.get(connStr) == null){
			URI uri;
			try {
				uri = new URI(connStr);
			} catch (URISyntaxException e2) {
				return null;
			}
			Jedis jedis = new Jedis(uri,15000);
			try {
				jedis.connect();
			} catch (Exception e) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					addConnection(connStr);
				}
			}
			
			connections.put(connStr, jedis);
			final Jedis connection = connections.get(connStr);
			
			ExecutorService es = Executors.newSingleThreadExecutor();
			watchTask.put(connection, es);
			es.submit(new Runnable(){

				@Override
				public void run() {
					try{
						connection.psubscribe(new JedisPubSub(){
							
							@Override
							public void onPMessage(String patten,String channel, String message) {
								String key = channel.substring(channel.lastIndexOf(":") + 1);
								YAEventType event;
								if(message.equals("expired") || message.equals("del")){
									event = YAEventType.DELETE;
								}else if(message.equals("set")){
									event = YAEventType.UPDATE;
								}else{
									return;
								}
								
								myself.notifyWatchers(ConnStrKeyUtil.makeLocation(connStr, key),event,DataFrom.REDIS);
							}
							
						},"__keyspace*");
					}catch(JedisConnectionException e){
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						connections.remove(connStr);
						watchTask.remove(connection);
						addConnection(connStr);
					}
				}
				
			});
		}
		
		return connections.get(connStr);
	}

	@Override
	public void unwatch(String location, WatcherListener... listeners) {
		boolean needUnRegister = unwatchLocal(location,listeners);
		
		if(needUnRegister){
			String connStr = ConnStrKeyUtil.getConnStrFromStr(location);
			boolean needRemove = true;
			for(Watcher w : watchers.values()){
				if(w.getKey().contains(connStr)){
					needRemove = false;
					break;
				}
			}
			if(needRemove){
				Jedis connection = connections.get(connStr);
				connection.close();
				watchTask.get(connection).shutdownNow();
				watchTask.remove(connection);
				connections.remove(connStr);
			}
		}
	}

}
