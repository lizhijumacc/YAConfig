package com.yaconfig.client.future;

public class YAFuture<V> extends AbstractFuture<V>{
	public long createTime;
	
	public YAFuture(){
		createTime = System.currentTimeMillis();
	}
}
