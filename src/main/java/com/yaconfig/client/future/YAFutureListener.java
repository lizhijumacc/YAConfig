package com.yaconfig.client.future;

import com.yaconfig.client.YAEntry;

public abstract class YAFutureListener implements FutureListener<YAEntry> {

	private String key;
	
	public YAFutureListener(String key){
		setKey(key);
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
	@Override
	public abstract void operationCompleted(AbstractFuture<YAEntry> abstractFuture);

}
