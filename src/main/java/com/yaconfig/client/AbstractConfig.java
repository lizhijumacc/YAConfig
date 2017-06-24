package com.yaconfig.client;

import java.lang.ref.SoftReference;

public class AbstractConfig {
	
	YAConfigClient client;
	
	public AbstractConfig(YAConfigClient client){
		this.client = client;
		client.registerConfig(new SoftReference<AbstractConfig>(this,client.queue));
	}
	
}
