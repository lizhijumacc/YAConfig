package com.leqicheng.yaconfig;

import com.yaconfig.client.AbstractConfig;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.RemoteValue;

public class TestConfig extends AbstractConfig{

	@RemoteValue(key = "com.test.0")
	public String value1;
	
	@RemoteValue(key = "com.test.1")
	public String value2;
	
	public TestConfig(YAConfigClient client) {
		super(client);
	}
	
	@BeforeChange(field = "value1")
	public void before1(){
		System.out.println("(1)old value1 is:" + value1);
	}

	@AfterChange(field = "value1")
	public void after1(){
		System.out.println("(1)new value1 is:" + value1);
	}
	
	@BeforeChange(field = "value1")
	public void before2(){
		System.out.println("(2)old value1 is:" + value1);
	}

	@AfterChange(field = "value1")
	public void after2(){
		System.out.println("(2)new value1 is:" + value1);
	}
	
	@BeforeChange(field = "value2")
	public void before3(){
		System.out.println("(2)old value2 is:" + value2);
	}

	@AfterChange(field = "value2")
	public void after3(){
		System.out.println("(2)new value2 is:" + value2);
	}
}
