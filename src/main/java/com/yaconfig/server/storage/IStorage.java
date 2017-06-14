package com.yaconfig.server.storage;

public interface IStorage {
	public int put(String key,byte[] value);
	
	public byte[] get(String key);
}
