package com.yaconfig.core;

public class YAConfig {
	private ValueInjector injector = ValueInjector.getInstance();
	
	public YAConfig(){
	}
	
	public void scanPackage(String scanPackage){
		this.injector.scan(scanPackage.split(Constants.NODE_SEPERATOR));
	}
}
