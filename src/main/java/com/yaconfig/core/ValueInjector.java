package com.yaconfig.core;

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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
import com.yaconfig.core.annotation.AfterChange;
import com.yaconfig.core.annotation.Anchor;
import com.yaconfig.core.annotation.BeforeChange;
import com.yaconfig.core.annotation.ControlChange;
import com.yaconfig.core.annotation.InitValueFrom;
import com.yaconfig.core.fetchers.FetchCallback;
import com.yaconfig.core.fetchers.Fetcher;
import com.yaconfig.core.fetchers.FetcherType;
import com.yaconfig.core.syncs.Sync;
import com.yaconfig.core.syncs.SyncType;
import com.yaconfig.core.watchers.FieldChangeCallback;
import com.yaconfig.core.watchers.Watchers;
import com.yaconfig.core.watchers.WatchersType;

public class ValueInjector implements FieldChangeCallback,FetchCallback {
	
	public static final int SOFT_GC_INTERVAL = 1000;
	
	private List<SoftReference<Object>> registry;
	public ReferenceQueue<Object> queue;
	private volatile static ValueInjector instance;
	private List<Watchers> watchers;
	private List<Fetcher> fetchers;
	private List<Sync> syncs;
	
	private Map<Field,Set<Method>> beforeInjectMethods;
	private Map<Field,Set<Method>> afterInjectMethods;
	private Map<Field,Set<Method>> controlInjectMethods;
	
	private static ScheduledExecutorService purgeTask;
	
	public static ValueInjector getInstance(){
		if(instance == null){
			synchronized(ValueInjector.class){
				if(instance == null){
					instance = new ValueInjector();
				}
			}
		}
		
		purgeTask = Executors.newSingleThreadScheduledExecutor();
		purgeTask.scheduleAtFixedRate(new Runnable(){

			@Override
			public void run() {
				instance.purgeSoftQueue();
			}
			
		},200, SOFT_GC_INTERVAL, TimeUnit.MILLISECONDS);
		
		return instance;
	}
	
	private ValueInjector(){
		this.registry = new LinkedList<SoftReference<Object>>();
		this.queue = new ReferenceQueue<Object>();
		this.beforeInjectMethods = new HashMap<Field,Set<Method>>();
		this.afterInjectMethods = new HashMap<Field,Set<Method>>();
		this.controlInjectMethods = new HashMap<Field,Set<Method>>();
		this.watchers = new ArrayList<Watchers>();
		this.fetchers = new ArrayList<Fetcher>(); 
		this.syncs = new ArrayList<Sync>();
		this.initScan();
	}
	
	private void initScan() {
		Reflections reflections = getReflection(new String[]{""});
		Set<Class<?>> clazzs = reflections.getTypesAnnotatedWith(WatchersType.class);
		for(Class<?> c : clazzs){
			try {
				this.watchers.add((Watchers)c.newInstance());
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		clazzs = reflections.getTypesAnnotatedWith(FetcherType.class);
		for(Class<?> c : clazzs){
			try {
				this.fetchers.add((Fetcher)c.newInstance());
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		clazzs = reflections.getTypesAnnotatedWith(SyncType.class);
		for(Class<?> c : clazzs){
			try {
				this.syncs.add((Sync)c.newInstance());
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	public synchronized void scan(String[] packageList) {
		Reflections reflections = getReflection(packageList);
		for(Watchers w : watchers){
			Set<Field> fs = w.init(reflections, this);
			registerFields(fs, reflections);
		}
	}

    private void initSetValue(Field field) {
    	InitValueFrom df = field.getAnnotation(InitValueFrom.class);
    	if(df == null){
    		return;
    	}
    	
    	this.fetchAndInjectNewValue(field, df.from());
    
	}

	private void registerFields(Set<Field> fields, Reflections reflections) {
		Set<Method> befores = reflections.getMethodsAnnotatedWith(BeforeChange.class);
		Set<Method> afters = reflections.getMethodsAnnotatedWith(AfterChange.class);
		Set<Method> controls = reflections.getMethodsAnnotatedWith(ControlChange.class);
		associate(fields,befores,beforeInjectMethods,BeforeChange.class);
		associate(fields,afters,afterInjectMethods,AfterChange.class);
		associate(fields,controls,controlInjectMethods,ControlChange.class);
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
	public void onAdd(String key, Field field, int from) {
		onUpdate(key,field,from);
	}

	@Override
	public void onUpdate(String key, Field field, int from) {
		Anchor anno = field.getAnnotation(Anchor.class);
		if(anno == null){
			fetchAndInjectNewValue(field,from);
		}else{
			int type = anno.anchor();
			if(from == type || type == AnchorType.LATEST){
				fetchAndInjectNewValue(field,from);
			}
		}
	}

	@Override
	public void onDelete(String key, Field field, int from) {
		try {
			injectAndSyncValue(null,field,from);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}
	
	public void fetchAndInjectNewValue(Field field, int from) {
    	for(Fetcher f : fetchers){
    		FetcherType ft = f.getClass().getAnnotation(FetcherType.class);
    		if(from == ft.from()){
    			f.fetch(field, this);
    		}
    	}
	}

	public void syncValue(String data, Field field, int from) {
		Anchor anchor = field.getAnnotation(Anchor.class);
		if(anchor != null && (from == anchor.anchor() || anchor.anchor() == AnchorType.LATEST)){
			syncValue0(data,field,from);
		}
	}

	private void syncValue0(String data, Field field, int from) {
		for(Sync s : this.syncs){
			SyncType st = s.getClass().getAnnotation(SyncType.class);
			if(st.from() != from){
				s.sync(data, field);
			}
		}
	}

	public void register(Object configProxy) {
		synchronized(registry){
			registry.add(new SoftReference<Object>(configProxy,this.queue));
		}
		Class<?> c = configProxy.getClass().getSuperclass();
		Field[] fs = c.getDeclaredFields();
		for(Field f : fs){
			initSetValue(f);
		}
	}
	
	public void injectAndSyncValue(String newValue,Field field,int from) {
		field.setAccessible(true);
		final Class<?> clazz = field.getDeclaringClass();
		
		String oldValue = null;
		synchronized(registry){
			for(SoftReference<Object> config : registry){
				Class<?> refClass = config.get().getClass().getSuperclass();
				if(clazz.equals(refClass)){
					try {
						oldValue = (String)field.get(config.get());
						if(oldValue != null && oldValue.equals(newValue)){
							return;
						}
					} catch (IllegalArgumentException | IllegalAccessException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		if(Modifier.isStatic(field.getModifiers())){
			try {
				inject(field.getClass(),field,newValue);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
		} else {
			synchronized(registry){
				for(SoftReference<Object> config : registry){
					if(clazz.equals(config.get().getClass().getSuperclass())){
						try {
							inject(config.get(),field,newValue);
						} catch (IllegalArgumentException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		syncValue(newValue,field,from);
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

	@Override
	public void dataFetched(String data, Field field, int from) {
		try {
			injectAndSyncValue(data,field,from);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}
}


