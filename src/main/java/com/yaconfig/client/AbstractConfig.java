package com.yaconfig.client;

public class AbstractConfig {
	
	YAConfigClient client;
	
	public AbstractConfig(YAConfigClient client){
		this.client = client;
		client.registerConfig(this);
	}
	
}
