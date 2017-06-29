package com.yaconfig.client;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.annotation.Use;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.annotation.SetValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.injector.ValueInjector;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.util.FileUtil;

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
			FileValue fv = f.getAnnotation(FileValue.class);
			RemoteValue rv = f.getAnnotation(RemoteValue.class);
			if(f != null){
				DataFrom from = gm.from();
				if(from != null){
					if(from.equals(DataFrom.FILE) && fv != null){
						injectDataFrom(fv.path() + "@" + fv.key(), f ,from);
					}
					if(from.equals(DataFrom.REMOTE) && rv != null){
						injectDataFrom(rv.key(), f ,from);
					}
				}else{
					Anchor anchor = f.getAnnotation(Anchor.class);
					if(anchor != null && anchor.anchor().equals(AnchorType.FILE)
							&& fv != null){
						injectDataFrom(fv.path() + "@" + fv.key(),f,DataFrom.FILE);
					}else if(anchor != null && anchor.anchor().equals(AnchorType.REMOTE)
							&& rv != null){
						injectDataFrom(rv.key(),f,DataFrom.REMOTE);
					}
				}
			}
		}
	}

	private void injectDataFrom(String key,Field field,DataFrom from) {
		ValueInjector.getInstance().fetchAndInjectNewValue(key, field, from);
	}

	private void processSetValue(Method method, Object object) throws Throwable {
		SetValue sa = method.getAnnotation(SetValue.class);
		if(sa != null && object != null){
			String fieldName = sa.field();
			String newValue = object.toString();
			Field f = method.getDeclaringClass().getDeclaredField(fieldName);
			if(f != null){
				Anchor anchor = f.getAnnotation(Anchor.class);
				if(anchor != null && anchor.anchor().equals(AnchorType.MEMORY)){
					FileValue fv = f.getAnnotation(FileValue.class);
					if(fv != null){
						FileUtil.writeValueToFile(fv.path(), fv.key(), newValue);
					}
					
					RemoteValue rv = f.getAnnotation(RemoteValue.class);
					if(rv != null){
						YAConfigClient.getInstance().put(rv.key(), newValue.getBytes(), YAMessage.Type.PUT_NOPROMISE);
					}
					
					ValueInjector.getInstance().injectValue(newValue,f);
				}
			}
		}
	}

}
