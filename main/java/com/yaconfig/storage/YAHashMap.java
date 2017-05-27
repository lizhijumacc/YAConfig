package com.yaconfig.storage;

import java.util.HashMap;

public class YAHashMap implements IStorage{

	private static volatile HashMap<String,byte[]> map;
	
	private static volatile YAHashMap instance;
	
	private YAHashMap(){}
	
	public static YAHashMap getInstance(){
		if(instance == null){
			synchronized(YAHashMap.class){
				if(instance == null){
					map = new HashMap<String,byte[]>();
					instance = new YAHashMap();
				}
			}
		}
		
		return instance;
	}
	
	public int put(String key, byte[] value) {
		map.put(key, value);
		return 0;
	}

	public byte[] get(String key) {
		return (byte[]) map.get(key);
	}
}
