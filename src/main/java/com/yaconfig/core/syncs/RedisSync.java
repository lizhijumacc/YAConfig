package com.yaconfig.core.syncs;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

import com.yaconfig.core.annotation.RedisValue;
import com.yaconfig.core.injector.DataFrom;

import redis.clients.jedis.Jedis;

@SyncType(from = DataFrom.REDIS)
public class RedisSync extends AbstractSync {

	@Override
	public void sync(String data,Field field,int from) {
		final RedisValue rdv = field.getAnnotation(RedisValue.class);
		if(rdv != null){
			Jedis jedis = null;
			try {
				jedis = new Jedis(new URI(rdv.connStr()), 15000);
				jedis.connect();
				jedis.set(rdv.key(), data);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			} finally {
				if(jedis != null){
					jedis.close();
				}
			}
		}
	}

}
