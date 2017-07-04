package com.yaconfig.client.injector;

import java.lang.annotation.Annotation;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
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
import com.yaconfig.client.annotation.MySQLValue;
import com.yaconfig.client.annotation.RedisValue;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.annotation.ZookeeperValue;
import com.yaconfig.client.fetcher.FetchCallback;
import com.yaconfig.client.fetcher.Fetcher;
import com.yaconfig.client.fetcher.FetcherType;
import com.yaconfig.client.fetcher.FileFetcher;
import com.yaconfig.client.fetcher.MySQLFetcher;
import com.yaconfig.client.fetcher.RedisFetcher;
import com.yaconfig.client.fetcher.RemoteFetcher;
import com.yaconfig.client.fetcher.ZookeeperFetcher;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.util.ConnStrKeyUtil;
import com.yaconfig.client.util.FileUtil;

import com.yaconfig.client.watchers.Watchers;
import com.yaconfig.client.watchers.WatchersType;


import redis.clients.jedis.Jedis;

public class ValueInjector implements FieldChangeCallback,FetchCallback {
	
	public static final int SOFT_GC_INTERVAL = 1000;
	
	private List<SoftReference<Object>> registry;
	public ReferenceQueue<Object> queue;
	private volatile static ValueInjector instance;
	private List<Watchers> watchers;
	private List<Fetcher> fetchers;
	
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
			if(from == type){
				fetchAndInjectNewValue(field,from);
			}
		}
	}

	@Override
	public void onDelete(String key, Field field, int from) {
		try {
			injectValue(null,field);
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

	protected void syncValue(String data, Field field, int from) {
		Anchor anchor = field.getAnnotation(Anchor.class);
		if(anchor != null){
			int anchorType = anchor.anchor();
			final FileValue fv = field.getAnnotation(FileValue.class);
			if(fv != null && anchorType == AnchorType.FILE && from == DataFrom.FILE){
				try {
					syncValue0(data,field,from);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
			
			final RemoteValue rv = field.getAnnotation(RemoteValue.class);
			if(rv != null && anchorType == AnchorType.REMOTE && from == DataFrom.REMOTE){
				try {
					syncValue0(data,field,from);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
			
			final ZookeeperValue zv = field.getAnnotation(ZookeeperValue.class);
			if(zv != null && anchorType == AnchorType.ZOOKEEPER && from == DataFrom.ZOOKEEPER){
				try {
					syncValue0(data,field,from);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
			
			final RedisValue rdv = field.getAnnotation(RedisValue.class);
			if(rdv != null && anchorType == AnchorType.REDIS && from == DataFrom.REDIS){
				try {
					syncValue0(data,field,from);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
			
			final MySQLValue mv = field.getAnnotation(MySQLValue.class);
			if(mv != null && anchorType == AnchorType.MYSQL && from == DataFrom.MYSQL){
				try {
					syncValue0(data,field,from);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void syncValue0(String data, Field field, int from) {
		final FileValue fv = field.getAnnotation(FileValue.class);
		final RemoteValue rv = field.getAnnotation(RemoteValue.class);
		final ZookeeperValue zv = field.getAnnotation(ZookeeperValue.class);
		final RedisValue rdv = field.getAnnotation(RedisValue.class);
		final MySQLValue mv = field.getAnnotation(MySQLValue.class);

		if(fv != null && from != DataFrom.FILE){
			FileUtil.writeValueToFile(ConnStrKeyUtil.makeLocation(fv.path(), fv.key()), data);
		}
		
		if(rv != null && from != DataFrom.REMOTE){
			YAConfigConnection connection = new YAConfigConnection();
			connection.attach(rv.connStr());
			connection.put(rv.key(), data.getBytes(), YAMessage.Type.PUT_NOPROMISE).awaitUninterruptibly();
			connection.detach();
		}
		
		if(rdv != null && from != DataFrom.REDIS){
			Jedis jedis = null;
			try {
				jedis = new Jedis(new URI(rdv.connStr()), 15000);
				jedis.connect();
				jedis.set(rdv.key(), data);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			} finally {
				if(jedis != null){
					jedis.close();
				}
			}
		}
		
		if(mv != null && from != DataFrom.MYSQL){
			String keyName = mv.keyName();
			String valueName = mv.valueName();
			String key = mv.key();
		    String username = mv.userName();
		    String password = mv.password();
			String tableName = mv.tableName();
			String connStr = mv.connStr();
			String sql = "update " + tableName + " set " + valueName + " = '" +
							data + "' where " + keyName + " = '" + key + "'";

		    Connection conn = null;
		    try {
		        conn = (Connection) DriverManager.getConnection(connStr, username, password);
		        PreparedStatement pstmt = (PreparedStatement)conn.prepareStatement(sql);
		        pstmt.execute();
		    } catch (SQLException e) {
		        e.printStackTrace();
		    }finally{
		    	try {
					if(conn!=null)conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		    }
		}
		
		if(zv != null && from != DataFrom.ZOOKEEPER){
			RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);
			CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(zv.connStr())
					.retryPolicy(policy).build();
			try {
				curator.start();
				curator.createContainers(zv.key());
				curator.setData().inBackground().forPath(zv.key(),data.getBytes());
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				curator.close();
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
	public void dataFetched(String data, Field field, int from) {
		try {
			injectValue(data,field);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		
		syncValue(data,field,from);
	}
}


