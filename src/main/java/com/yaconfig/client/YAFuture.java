package com.yaconfig.client;

public class YAFuture<V> extends AbstractFuture<V>{
	public long createTime;
	
	public YAFuture(){
		createTime = System.currentTimeMillis();
	}
}
