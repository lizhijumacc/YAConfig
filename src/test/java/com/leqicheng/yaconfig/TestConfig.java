package com.leqicheng.yaconfig;

import com.yaconfig.client.AbstractConfig;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.RemoteValue;

public class TestConfig extends AbstractConfig{

	@RemoteValue(key = "com.test.*")
	public String value;
	
	public TestConfig(YAConfigClient client) {
		super(client);
	}
	
	@BeforeChange(field = "value")
	public void before1(){
		System.out.println("(1)old value is:" + value);
	}

	@AfterChange(field = "value")
	public void after1(){
		System.out.println("(1)new value is:" + value);
	}
	
	@BeforeChange(field = "value")
	public void before2(){
		System.out.println("(2)old value is:" + value);
	}

	@AfterChange(field = "value")
	public void after2(){
		System.out.println("(2)new value is:" + value);
	}
}
