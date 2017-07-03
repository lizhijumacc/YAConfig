package com.yaconfig.client.watchers;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.yaconfig.client.Constants;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.ZookeeperValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.injector.FieldChangeCallback;
import com.yaconfig.client.injector.FieldChangeListener;
import com.yaconfig.client.util.ConnStrKeyUtil;

public class ZookeeperWatchers extends AbstractWatchers {
	
	HashMap<String,CuratorFramework> connections = new HashMap<String,CuratorFramework>();
	HashMap<CuratorFramework,Set<TreeCache>> connectionMapCaches = new HashMap<CuratorFramework,Set<TreeCache>>();
	HashMap<TreeCache,String> cacheMapKeys = new HashMap<TreeCache,String>();
	
	public ZookeeperWatchers myself;

	public ZookeeperWatchers(Set<Field> fields,FieldChangeCallback callback){
		myself = this;
		for(final Field field : fields){
			ZookeeperValue annotation = field.getAnnotation(ZookeeperValue.class);
			String key = annotation.key();
			String connStr = annotation.connStr();
			Anchor anchor = field.getAnnotation(Anchor.class);
			if(anchor == null || anchor != null && anchor.anchor().equals(AnchorType.ZOOKEEPER)){
				watch(connStr + Constants.CONNECTION_KEY_SEPERATOR + key,new FieldChangeListener(field,callback));
			}
		}
	}
	
	@Override
	public void watch(String location, WatcherListener... listeners) {
		boolean needRegister = watchLocal(location, listeners);
		if(!needRegister){
			return;
		}
		
		String connStr = ConnStrKeyUtil.getConnStrFromStr(location);
		final String key = ConnStrKeyUtil.getKeyNameFromStr(location);
		
		if(!connections.containsKey(connStr)){
			addConnection(connStr);
		}
		
		TreeCache cache = new TreeCache (connections.get(connStr), key);
		try {
			cache.start();
			cache.getListenable().addListener(new TreeCacheListener(){

				@Override
				public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
					String location = ConnStrKeyUtil.makeLocation(client.getZookeeperClient().getCurrentConnectionString(), key);
					switch (event.getType()) {
						case NODE_ADDED:
							myself.notifyWatchers(location, YAEventType.ADD, DataFrom.ZOOKEEPER);
							break;
						case NODE_UPDATED:
							myself.notifyWatchers(location, YAEventType.UPDATE, DataFrom.ZOOKEEPER);
							break;
						case NODE_REMOVED:
							myself.notifyWatchers(location, YAEventType.DELETE, DataFrom.ZOOKEEPER);
							break;
						default:
							break;
					}
				}
				
			});
			addCache(connections.get(connStr),cache,key);
		} catch (Exception e) {
			cache.close();
			e.printStackTrace();
		}
	}

	private void addCache(CuratorFramework curatorFramework, TreeCache cache,String key) {
		if(cacheMapKeys.containsValue(key)){
			return;
		}
		if(connectionMapCaches.get(curatorFramework) == null){
			HashSet<TreeCache> cacheSet = new HashSet<TreeCache>();
			cacheSet.add(cache);
			connectionMapCaches.put(curatorFramework, cacheSet);
		}else{
			connectionMapCaches.get(curatorFramework).add(cache);
		}
		cacheMapKeys.put(cache, key);
	}

	private void addConnection(String connStr) {
		if(!connections.containsKey(connStr)){
			RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);
			CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(connStr).retryPolicy(policy).build();  
			curator.start();
			connections.put(connStr, curator);
			
			curator.getConnectionStateListenable().addListener(new ConnectionStateListener(){

				@Override
				public void stateChanged(CuratorFramework client, ConnectionState newState) {
					if(newState == ConnectionState.LOST){
						//System.out.println("connect lost");
					}else if(newState == ConnectionState.CONNECTED){
						//System.out.println("connect zookeeper!");
					}else if(newState == ConnectionState.RECONNECTED){
						//System.out.println("reconnected zookeeper!");
					}
				}
				
			});
		}
	}
	
	@Override
	public void unwatch(String location, WatcherListener... listeners) {
		boolean needUnRegister = unwatchLocal(location,listeners);

		if(needUnRegister){
			String connStr = ConnStrKeyUtil.getConnStrFromStr(location);
			String key = ConnStrKeyUtil.getKeyNameFromStr(location);
			CuratorFramework connection = connections.get(connStr);
			if(connection != null){
				Set<TreeCache> tcs = connectionMapCaches.get(connection);
				for(TreeCache tc : tcs){
					if(cacheMapKeys.get(tc).equals(key)){
						tc.close();
						cacheMapKeys.remove(tc);
						tcs.remove(tc);
						break;
					}
				}
				if(tcs.size() == 0){
					connection.close();
					connectionMapCaches.remove(connection);
					connections.remove(connection);
				}
			}
		}
	}

}
