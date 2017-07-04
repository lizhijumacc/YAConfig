package com.yaconfig.core;

import com.yaconfig.core.injector.ValueInjector;

public class YAConfig {
	private ValueInjector injector = ValueInjector.getInstance();
	
	public YAConfig(){
	}
	
	public void scanPackage(String scanPackage){
		this.injector.scan(scanPackage.split(Constants.NODE_SEPERATOR));
	}
}
