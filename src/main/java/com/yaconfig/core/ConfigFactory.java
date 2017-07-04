package com.yaconfig.core;

import org.apache.log4j.PropertyConfigurator;

import com.yaconfig.core.injector.ValueInjector;

import net.sf.cglib.proxy.Enhancer;

public class ConfigFactory {
	public static Object getConfig(Class<?> configClass){
		PropertyConfigurator.configure("log4j.properties");
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(configClass);
		enhancer.setCallback(new YAMethodInterceptor());
		Object proxy = enhancer.create();
		ValueInjector.getInstance().register(proxy);
		return proxy;
	}
}
