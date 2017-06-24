package com.leqicheng.yaconfig;

import com.yaconfig.client.AbstractConfig;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.YAConfigValue;

public class TestConfig extends AbstractConfig{

	public TestConfig(YAConfigClient client) {
		super(client);
	}
	
	@YAConfigValue(key = "com.test.*")
	public String value;

}
