package com.yaconfig.test;

import com.yaconfig.client.AbstractConfig;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.ControlChange;
import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.annotation.InitValueFrom;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.DataFrom;

public class TestConfig extends AbstractConfig{

	@RemoteValue(key = "com.test.6")
	public String value1;
	
	@RemoteValue(key = "com.test.6")
	public String value2;
	
	@RemoteValue(key = "com.test.0")
	@FileValue(key = "connectStr", path = "D:\\test\\test.config")
	@Anchor(anchor = AnchorType.FILE)
	@InitValueFrom(from = DataFrom.REMOTE)
	public String value3;
	
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
		System.out.println("(3)old value2 is:" + value2);
	}

	@AfterChange(field = "value2")
	public void after3(){
		System.out.println("(3)new value2 is:" + value2);
	}
	
	@ControlChange(field = "value2")
	public Boolean control4(String newValue){
		System.out.println("(4)controlChange is:" + value2);
		if(value2 != null && newValue.equals(value2)){
			return false;
		}
		return true;
	}
	
	@ControlChange(field = "value2")
	public Boolean control5(String newValue){
		System.out.println("(5)controlChange is always true");
		return true;
	}
	
	@BeforeChange(field = "value3")
	public void before6(){
		//System.out.println("(6)old value3 is:" + value3);
	}

	@AfterChange(field = "value3")
	public void after6(){
		System.out.println("(6)new value3 is:" + value3);
		if(value3 == null){
			System.out.println("????");
		}
	}
}
