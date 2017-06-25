package com.yaconfig.client.injector;

import java.lang.annotation.Annotation;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.google.common.base.Predicate;
import com.yaconfig.client.AbstractConfig;
import com.yaconfig.client.Constants;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.YAEntry;
import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.ControlChange;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.future.AbstractFuture;
import com.yaconfig.client.future.YAFuture;
import com.yaconfig.client.future.YAFutureListener;
import com.yaconfig.client.message.YAMessage;

public class ValueInjector implements FieldChangeCallback {
	
	private List<SoftReference<AbstractConfig>> registry;
	public ReferenceQueue<AbstractConfig> queue;
	private YAConfigClient client;
	private ValueInjector myself;
	
	private Set<Field> fields;
	private Map<Field,Set<Method>> beforeInjectMethods;
	private Map<Field,Set<Method>> afterInjectMethods;
	private Map<Field,Set<Method>> controlInjectMethods;
	
	public ValueInjector(YAConfigClient client){
		this.registry = new LinkedList<SoftReference<AbstractConfig>>();
		this.queue = new ReferenceQueue<AbstractConfig>();
		this.client = client;
		this.beforeInjectMethods = new HashMap<Field,Set<Method>>();
		this.afterInjectMethods = new HashMap<Field,Set<Method>>();
		this.controlInjectMethods = new HashMap<Field,Set<Method>>();
		myself = this;
	}
	
	public synchronized void scan(String[] packageList) {
		Reflections reflections = getReflection(packageList);
		
		fields = reflections.getFieldsAnnotatedWith(RemoteValue.class);
		Set<Method> befores = reflections.getMethodsAnnotatedWith(BeforeChange.class);
		Set<Method> afters = reflections.getMethodsAnnotatedWith(AfterChange.class);
		Set<Method> controls = reflections.getMethodsAnnotatedWith(ControlChange.class);
		associate(fields,befores,beforeInjectMethods,BeforeChange.class);
		associate(fields,afters,afterInjectMethods,AfterChange.class);
		associate(fields,controls,controlInjectMethods,ControlChange.class);
		
		for(final Field field : fields){
			RemoteValue annotation = field.getAnnotation(RemoteValue.class);
			String key = annotation.key();
			YAFuture<YAEntry> f = client.watch(key,new FieldChangeListener(field,this));
			
			f.addListener(new YAFutureListener(key){

				@Override
				public void operationCompleted(AbstractFuture<YAEntry> future) {
					if(!future.isSuccess()){
						System.out.println("Watch KEY:" + this.getKey() + " ERROR in scanning.");
						try {
							future.get();
						} catch (InterruptedException | ExecutionException e) {
							System.out.println(e.getCause().getMessage());
						}
					}else{
						System.out.println("Watch KEY:" + this.getKey() + " successfully in scanning.");
					}
				}
				
			});

		}
	}

    private void associate(Set<Field> fields, Set<Method> methods, Map<Field, Set<Method>> map,Class<? extends Annotation> annotationClass) {
		for(Field f : fields){
			for(Method m : methods){
				String fieldName;
				Object anno = m.getAnnotation(annotationClass);
				if(annotationClass.equals(BeforeChange.class)){
					fieldName = ((BeforeChange)anno).field();
				}else if(annotationClass.equals(AfterChange.class)){
					fieldName = ((AfterChange)anno).field();
				}else if(annotationClass.equals(ControlChange.class)){
					fieldName = ((ControlChange)anno).field();
				}else{
					return;
				}
				
				if(f.getDeclaringClass().equals(m.getDeclaringClass())
						&& f.getName().equals(fieldName)){
					if(!map.containsKey(f)){
						Set<Method> ms = new HashSet<Method>();
						ms.add(m);
						map.put(f, ms);
					}else{
						Set<Method> ms = map.get(f);
						ms.add(m);
					}
				}
			}
		}
	}

	private Reflections getReflection(String[] packNameList) {

        FilterBuilder filterBuilder = new FilterBuilder().includePackage(Constants.YACONFIG_PACK_NAME);

        for (String packName : packNameList) {
            filterBuilder = filterBuilder.includePackage(packName);
        }
        Predicate<String> filter = filterBuilder;

        Collection<URL> urlTotals = new ArrayList<URL>();
        for (String packName : packNameList) {
            Collection<URL> urls = ClasspathHelper.forPackage(packName);
            urlTotals.addAll(urls);
        }

        Reflections reflections = new Reflections(new ConfigurationBuilder().filterInputsBy(filter)
                .setScanners(new SubTypesScanner().filterResultsBy(filter),
                        new TypeAnnotationsScanner()
                                .filterResultsBy(filter),
                        new FieldAnnotationsScanner()
                                .filterResultsBy(filter),
                        new MethodAnnotationsScanner()
                                .filterResultsBy(filter),
                        new MethodParameterScanner()).setUrls(urlTotals));

        return reflections;
    }

	@Override
	public void onAdd(String key, Field field) {
		fetchAndInjectNewValue(key,field);
	}

	@Override
	public void onUpdate(String key, Field field) {
		fetchAndInjectNewValue(key,field);
	}

	@Override
	public void onDelete(String key, Field field) {
		try {
			myself.injectValue(new YAEntry(key,null),field);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}
	
	private void fetchAndInjectNewValue(String key,final Field field){
		YAFuture<YAEntry> f = client.get(key, YAMessage.Type.GET_LOCAL);
		f.addListener(new YAFutureListener(key){

			@Override
			public void operationCompleted(AbstractFuture<YAEntry> future) {
				if(future.isSuccess()){
					try {
						myself.injectValue(future.get(),field);
					} catch (IllegalArgumentException | InterruptedException
							| ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
			
		});

	}

	public void register(AbstractConfig config) {
		synchronized(registry){
			registry.add(new SoftReference<AbstractConfig>(config,this.queue));
		}
	}
	
	private void injectValue(YAEntry yaEntry,Field field) {
		if(Modifier.isStatic(field.getModifiers())){
			try {
				inject(field.getClass(),field,new String(yaEntry.getValue()));
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
		} else {
			final Class<?> clazz = field.getDeclaringClass();
			
			synchronized(registry){
				for(SoftReference<AbstractConfig> config : registry){
					
					if(clazz.equals(config.get().getClass())){
						try {
							inject(config.get(),field,new String(yaEntry.getValue()));
						} catch (IllegalArgumentException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}

	private void inject(Object obj, Field field, Object newValue) {
		boolean accept = true;
		if(this.controlInjectMethods.containsKey(field)){
			Set<Method> ms = this.controlInjectMethods.get(field);
			for(Method m : ms){
				try {
					Object res;
					res = m.invoke(obj,newValue);
					if(res != null && res instanceof Boolean){
						Boolean isAccept = (Boolean)res;
						accept = isAccept.booleanValue();
						if(!accept){
							break;
						}
					}
					
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					e.printStackTrace();
				}
			}
			

		}
		
		if(accept){
			invokeMethods(this.beforeInjectMethods,field,obj);
			try {
				field.set(obj, newValue);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
			invokeMethods(this.afterInjectMethods,field,obj);
		}
	}

	private void invokeMethods(Map<Field, Set<Method>> maps,Field field, Object obj) {
		if(!maps.containsKey(field)){
			return;
		}else{
			Set<Method> ms = maps.get(field);
			for(Method m : ms){
				try {
					m.invoke(obj);
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void purgeSoftQueue() {
		while(true){  
	        Object softRef = queue.poll();  
	        if(softRef == null){  
	                break;  
	        }  
	        registry.remove(softRef);  
		}  
	}
}


