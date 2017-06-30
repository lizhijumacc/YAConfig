package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.util.ConnStrKeyUtil;

public class ZookeeperFetcher extends AbstractFetcher{

	public ZookeeperFetcher(Field field) {
		super(field);
	}

	@Override
	public void fetch(String location, FetchCallback callback) {
		String connStr = ConnStrKeyUtil.getConnStrFromStr(location);
		String key = ConnStrKeyUtil.getKeyNameFromStr(location);
		
		RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);  
	    CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(connStr).retryPolicy(policy).build(); 
	    try {
	    	String data = new String(curator.getData().forPath(key));
			callback.dataFetched(data, field, DataFrom.ZOOKEEPER);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
