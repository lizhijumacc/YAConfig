package com.yaconfig.server;

public interface IChangeListener {
	public void onChange(String key,byte[] value,int type);
}
