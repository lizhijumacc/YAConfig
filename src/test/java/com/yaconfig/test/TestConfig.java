package com.yaconfig.test;

import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.ControlChange;
import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.annotation.Use;
import com.yaconfig.client.annotation.InitValueFrom;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.annotation.SetValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.DataFrom;

public class TestConfig{

	@RemoteValue(key = "com.test.6")
	private String value1;
	
	@RemoteValue(key = "com.test.6")
	private String value2;
	
	@RemoteValue(key = "com.test.0")
	@FileValue(key = "connectStr", path = "D:\\test\\test.config")
	@Anchor(anchor = AnchorType.MEMORY)
	@InitValueFrom(from = DataFrom.FILE)
	private String value3;
	
	public TestConfig() {

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
	}
	
	@SetValue(field = "value3")
	public void setValue3(String newValue){
		//this.value3 = newValue;
		//before this method, newValue will inject into value3 automatically & invoke @Before&AfterChange method
		//no necessary to execute like "this.value3 = newValue"
		//System.out.println("new Value is" + value3);
	}
	
	@Use(field = "value3",from = DataFrom.REMOTE)
	public void useValue3FromRemote(){
		System.out.println("use remote value");
		//should get last value in method with @AfterChange annotation.
	}
	
	@Use(field = "value3",from = DataFrom.FILE)
	public void useValue3FromFile(){
		System.out.println("use file value");
		//should get last value in method with @AfterChange annotation.
	}
	
	public String getValueFromFile(String file,String key){
		return null;
	}
	
	public String getValueFromRemote(String key){
		return null;
	}
}
