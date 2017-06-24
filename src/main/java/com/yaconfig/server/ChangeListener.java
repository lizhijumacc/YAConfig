package com.yaconfig.server;

public interface ChangeListener {
	public void onChange(String key,byte[] value,int type);
}
