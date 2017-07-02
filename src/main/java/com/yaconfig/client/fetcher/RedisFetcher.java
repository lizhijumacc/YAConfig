package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.util.ConnStrKeyUtil;

import redis.clients.jedis.Jedis;

public class RedisFetcher extends AbstractFetcher{

	public RedisFetcher(Field field) {
		super(field);
	}

	@Override
	public void fetch(String location, FetchCallback callback) {
		String connStr = ConnStrKeyUtil.getConnStrFromStr(location);
		String key = ConnStrKeyUtil.getKeyNameFromStr(location);
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
