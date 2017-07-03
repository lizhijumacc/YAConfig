package com.yaconfig.test;

import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.ControlChange;
import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.annotation.Use;
import com.yaconfig.client.annotation.ZookeeperValue;
import com.yaconfig.client.annotation.InitValueFrom;
import com.yaconfig.client.annotation.MySQLValue;
import com.yaconfig.client.annotation.RedisValue;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.annotation.SetValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.DataFrom;

public class TestConfig{

	//@RemoteValue(key = "com.test.0" ,connStr = "127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890")
	//@Anchor(anchor = AnchorType.REMOTE)
	private String value1;
	
	private String value2;
	
	@RemoteValue(key = "com.test.0" ,connStr = "127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890")
	private String remoteValue;
	
	@FileValue(key = "connectStr", path = "D:\\test\\test.config")
	private String fileValue;
	
	@RedisValue(connStr = "redis://127.0.0.1:6379/0", key = "testConfig")
	private String redisValue;
	
	@MySQLValue(connStr = "jdbc:mysql://127.0.0.1:3306/world?useSSL=false&serverTimezone=UTC", tableName = "testConfig",
			valueName = "testConfigValue", keyName = "testConfigName",
			key = "connStr",
			userName = "root", password = "jmdlbl88")
	private String mysqlValue;
	
	@ZookeeperValue(connStr = "127.0.0.1:2181", key = "/testConfig1/test")
	private String zookeeperValue;
	
	@RemoteValue(key = "com.test.0" ,connStr = "127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890")
	@FileValue(key = "connectStr", path = "D:\\test\\test.config")
	@MySQLValue(connStr = "jdbc:mysql://127.0.0.1:3306/world?useSSL=false&serverTimezone=UTC", tableName = "testConfig",
			valueName = "testConfigValue", keyName = "testConfigName",
			key = "connStr",
			userName = "root", password = "jmdlbl88")
	@RedisValue(connStr = "redis://127.0.0.1:6379/0", key = "testConfig")
	@ZookeeperValue(connStr = "127.0.0.1:2181", key = "/testConfig1/test")
	@Anchor(anchor = AnchorType.FILE)
	@InitValueFrom(from = DataFrom.FILE)
	private String value3;
	
	public TestConfig() {
		
	}
	
	@AfterChange(field = "remoteValue")
	public void remoteChange(){
		System.out.println("remoteValue change to:" + this.remoteValue);
	}
	
	@AfterChange(field = "fileValue")
	public void fileChange(){
		System.out.println("fileValue change to:" + this.fileValue);
	}
	
	@AfterChange(field = "redisValue")
	public void redisChange(){
		System.out.println("redisValue change to:" + this.redisValue);
	}
	
	@AfterChange(field = "mysqlValue")
	public void mysqlChange(){
		System.out.println("mysqlValue change to:" + this.mysqlValue);
	}
	
	@AfterChange(field = "zookeeperValue")
	public void zookeeperChange(){
		System.out.println("zookeeperValue change to:" + this.zookeeperValue);
	}
	
	@BeforeChange(field = "value1")
	public void before1(){
		//System.out.println("(1)old value1 is:" + value1);
	}

	@AfterChange(field = "value1")
	public void after1(){
		System.out.println("(1)new value1 is:" + value1);
	}
	
	@BeforeChange(field = "value1")
	public void before2(){
		//System.out.println("(2)old value1 is:" + value1);
	}

	@AfterChange(field = "value1")
	public void after2(){
		//System.out.println("(2)new value1 is:" + value1);
	}
	
	@BeforeChange(field = "value2")
	public void before3(){
		//System.out.println("(3)old value2 is:" + value2);
	}

	@AfterChange(field = "value2")
	public void after3(){
		System.out.println("com.test.0 on SERVER is:" + value2);
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
