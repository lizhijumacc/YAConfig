package com.yaconfig.core;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.yaconfig.core.annotation.Anchor;
import com.yaconfig.core.annotation.SetValue;
import com.yaconfig.core.annotation.Use;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class YAMethodInterceptor implements MethodInterceptor {

	@Override
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		if(args.length > 0){
			processSetValue(method,args[0]);
		}
		processUse(method);
		return proxy.invokeSuper(obj, args);
	}

	private void processUse(Method method) throws Throwable {
		Use gm = method.getAnnotation(Use.class);
		if(gm != null){
			String fieldName = gm.field();
			Field f = method.getDeclaringClass().getDeclaredField(fieldName);
			Anchor anchor = f.getAnnotation(Anchor.class);
			if(anchor != null){
				injectDataFrom(f,anchor.anchor());
			}else{
				injectDataFrom(f,gm.from());
			}
		}
	}

	private void injectDataFrom(Field field,int from) {
		ValueInjector.getInstance().fetchAndInjectNewValue(field, from);
	}

	private void processSetValue(Method method, Object object) throws Throwable {
		SetValue sa = method.getAnnotation(SetValue.class);
		if(sa != null && object != null){
			String fieldName = sa.field();
			String newValue = object.toString();
			Field f = method.getDeclaringClass().getDeclaredField(fieldName);
			if(f != null){
				Anchor anchor = f.getAnnotation(Anchor.class);
				if(anchor != null && anchor.anchor() == AnchorType.MEMORY){
					ValueInjector.getInstance().injectAndSyncValue(newValue, f, AnchorType.MEMORY);
				}
			}
		}
	}

}
