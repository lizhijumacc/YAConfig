package com.yaconfig.client;

import com.yaconfig.client.injector.ValueInjector;

import net.sf.cglib.proxy.Enhancer;

public class ConfigFactory {
	public static Object getConfig(Class<?> configClass){
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(configClass);
		enhancer.setCallback(new YAMethodInterceptor());
		Object proxy = enhancer.create();
		ValueInjector.getInstance().register(proxy);
		return proxy;
	}
}