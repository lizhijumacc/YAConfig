package com.yaconfig.core.fetchers;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

import com.yaconfig.core.DataFrom;
import com.yaconfig.core.annotation.RedisValue;

import redis.clients.jedis.Jedis;

@FetcherType(from = DataFrom.REDIS)
public class RedisFetcher extends AbstractFetcher{

	public RedisFetcher() {
		
	}

	@Override
	public void fetch(Field field,FetchCallback callback) {
		RedisValue rdv = field.getAnnotation(RedisValue.class);
		String key = rdv.key();
		String connStr = rdv.connStr();
		Jedis jedis = null;
	
		try {
			jedis = new Jedis(new URI(connStr), 15000);
			jedis.connect();
			String data = jedis.get(key);
			callback.dataFetched(data, field, DataFrom.REDIS);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} finally {
			if(jedis != null){
				jedis.close();
			}
		}
	}

}
