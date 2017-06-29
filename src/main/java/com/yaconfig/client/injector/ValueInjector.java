package com.yaconfig.client.injector;

import java.lang.annotation.Annotation;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Paths;
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
import com.yaconfig.client.Constants;
import com.yaconfig.client.YAConfigConnection;
import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.ControlChange;
import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.annotation.InitValueFrom;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.fetcher.FetchCallback;
import com.yaconfig.client.fetcher.FileFetcher;
import com.yaconfig.client.fetcher.RemoteFetcher;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.util.ConnStrKeyUtil;
import com.yaconfig.client.util.FileUtil;
import com.yaconfig.client.watchers.FileWatchers;
import com.yaconfig.client.watchers.MySQLWatchers;
import com.yaconfig.client.watchers.RedisWatchers;
import com.yaconfig.client.watchers.RemoteWatchers;
import com.yaconfig.client.watchers.ZookeeperWatchers;

public class ValueInjector implements FieldChangeCallback,FetchCallback {
	
	public static final int SOFT_GC_INTERVAL = 1000;
	
	private List<SoftReference<Object>> registry;
	public ReferenceQueue<Object> queue;
	private volatile static ValueInjector instance;
	private RemoteWatchers remoteWatchers;
	private FileWatchers fileWatchers;
	private MySQLWatchers mySQLWatchers;
	private RedisWatchers redisWatchers;
	private ZookeeperWatchers zookeeperWatchers;
	
	private Set<Field> fields;
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
	}
	
	public synchronized void scan(String[] packageList) {
		Reflections reflections = getReflection(packageList);
		
		//remote values
		fields = reflections.getFieldsAnnotatedWith(RemoteValue.class);
		if(fields.size() != 0){
			RemoteWatchers remoteWatchers = new RemoteWatchers(fields,this);
			this.remoteWatchers = remoteWatchers;
			registerFields(fields,reflections);
		}
		
		//file values
		fields = reflections.getFieldsAnnotatedWith(FileValue.class);
		if(fields.size() != 0){
			FileWatchers fileWatchers = new FileWatchers(fields,this);
			this.fileWatchers = fileWatchers;
			registerFields(fields,reflections);
		}
		
		//inject static values
		fields = reflections.getFieldsAnnotatedWith(InitValueFrom.class);
		for(Field field : fields){
			initSetValue(field);
		}
	}

    private void initSetValue(Field field) {
    	InitValueFrom df = field.getAnnotation(InitValueFrom.class);
    	if(df == null){
    		return;
    	}

		if(df.from().equals(DataFrom.FILE)){
			FileValue fv = field.getAnnotation(FileValue.class);
			if(fv != null){
				String path_r = field.getAnnotation(FileValue.class).path();
				String path = Paths.get(path_r).toAbsolutePath().toString();
				String key = field.getAnnotation(FileValue.class).key();
				fetchAndInjectNewValue(ConnStrKeyUtil.makeLocation(path, key), field, DataFrom.FILE);
			}
		}else if(df.from().equals(DataFrom.REMOTE)){
			RemoteValue rv = field.getAnnotation(RemoteValue.class);
			if(rv != null){
				String key = rv.key();
				String connStr = rv.connStr();
				fetchAndInjectNewValue(ConnStrKeyUtil.makeLocation(connStr, key), field, DataFrom.REMOTE);
			}
		}
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
	public void onAdd(String key, Field field, DataFrom from) {
		onUpdate(key,field,from);
	}

	@Override
	public void onUpdate(String key, Field field, DataFrom from) {
		Anchor anno = field.getAnnotation(Anchor.class);
		if(anno == null){
			fetchAndInjectNewValue(key,field,from);
		}else{
			AnchorType type = anno.anchor();
			if(from.equals(DataFrom.REMOTE) && type.equals(AnchorType.REMOTE)
					|| from.equals(DataFrom.FILE) && type.equals(AnchorType.FILE)){
				fetchAndInjectNewValue(key,field,from);
			}else{
				return;
			}
		}
	}

	@Override
	public void onDelete(String key, Field field, DataFrom from) {
		try {
			injectValue(null,field);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}
	
	public void fetchAndInjectNewValue(final String key,final Field field,final DataFrom from){
		if(from.equals(DataFrom.REMOTE)){
			RemoteFetcher remoteFetcher = new RemoteFetcher(field);
			remoteFetcher.fetch(key, this);
		}else if(from.equals(DataFrom.FILE)){
			FileFetcher fileFetcher = new FileFetcher(field);
			fileFetcher.fetch(key, this);
		}
	}

	protected void syncValue(String data, Field field, DataFrom from) {
		Anchor anchor = field.getAnnotation(Anchor.class);
		if(anchor != null){
			AnchorType anchorType = anchor.anchor();
			final FileValue fv = field.getAnnotation(FileValue.class);
			if(fv != null && anchorType.equals(AnchorType.FILE) && from.equals(DataFrom.FILE)){
				try {
					syncValue0(data,field,from);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
			
			final RemoteValue rv = field.getAnnotation(RemoteValue.class);
			if(rv != null && anchorType.equals(AnchorType.REMOTE) && from.equals(DataFrom.REMOTE)){
				try {
					syncValue0(data,field,from);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void syncValue0(String data, Field field, DataFrom from) {
		final FileValue fv = field.getAnnotation(FileValue.class);
		final RemoteValue rv = field.getAnnotation(RemoteValue.class);

		if(fv != null && from != DataFrom.FILE){
			FileUtil.writeValueToFile(ConnStrKeyUtil.makeLocation(fv.path(), fv.key()), data);
		}
		
		if(rv != null && from != DataFrom.REMOTE){
			YAConfigConnection connection = new YAConfigConnection();
			connection.attach(rv.connStr());
			connection.put(rv.key(), data.getBytes(), YAMessage.Type.PUT_NOPROMISE).awaitUninterruptibly();
			connection.detach();
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
	
	public void injectValue(String value,Field field) {
		field.setAccessible(true);
		if(Modifier.isStatic(field.getModifiers())){
			try {
				inject(field.getClass(),field,value);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
		} else {
			final Class<?> clazz = field.getDeclaringClass();
			
			synchronized(registry){
				for(SoftReference<Object> config : registry){
					if(clazz.equals(config.get().getClass().getSuperclass())){
						try {
							inject(config.get(),field,value);
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

	@Override
	public void dataFetched(String data, Field field, DataFrom from) {
		try {
			injectValue(data,field);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		syncValue(data,field,from);
	}
}


