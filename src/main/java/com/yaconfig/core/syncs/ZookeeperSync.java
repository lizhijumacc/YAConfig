package com.yaconfig.core.syncs;

import java.lang.reflect.Field;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.yaconfig.core.annotation.ZookeeperValue;
import com.yaconfig.core.injector.DataFrom;

@SyncType(from = DataFrom.ZOOKEEPER)
public class ZookeeperSync extends AbstractSync {

	@Override
	public void sync(String data,Field field,int from) {
		final ZookeeperValue zv = field.getAnnotation(ZookeeperValue.class);
		if(zv != null){
			RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);
			CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(zv.connStr())
					.retryPolicy(policy).build();
			try {
				curator.start();
				curator.createContainers(zv.key());
				curator.setData().inBackground().forPath(zv.key(),data.getBytes());
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				curator.close();
			}
		}
	}

}
