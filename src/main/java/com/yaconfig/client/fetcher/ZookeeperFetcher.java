package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.yaconfig.client.annotation.ZookeeperValue;
import com.yaconfig.client.injector.DataFrom;

@FetcherType(from = DataFrom.ZOOKEEPER)
public class ZookeeperFetcher extends AbstractFetcher{

	public ZookeeperFetcher() {
		
	}

	@Override
	public void fetch(Field field,FetchCallback callback) {
		ZookeeperValue zv = field.getAnnotation(ZookeeperValue.class);
		String key = zv.key();
		String connStr = zv.connStr();
		
		RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);  
	    CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(connStr).retryPolicy(policy).build(); 
	    try {
	    	curator.start();
	    	String data = new String(curator.getData().forPath(key));
			callback.dataFetched(data, field, DataFrom.ZOOKEEPER);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			curator.close();
		}
	}

}
