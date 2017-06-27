package com.yaconfig.client.injector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.google.common.io.Files;
import com.yaconfig.client.AbstractConfig;
import com.yaconfig.client.Constants;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.YAEntry;
import com.yaconfig.client.annotation.AfterChange;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.BeforeChange;
import com.yaconfig.client.annotation.ControlChange;
import com.yaconfig.client.annotation.FileValue;
import com.yaconfig.client.annotation.InitValueFrom;
import com.yaconfig.client.annotation.RemoteValue;
import com.yaconfig.client.future.AbstractFuture;
import com.yaconfig.client.future.YAFuture;
import com.yaconfig.client.future.YAFutureListener;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.storage.FileWatcher;

public class ValueInjector implements FieldChangeCallback {
	
	private List<SoftReference<AbstractConfig>> registry;
	public ReferenceQueue<AbstractConfig> queue;
	private YAConfigClient client;
	private ValueInjector myself;
	private FileWatcher fileWatcher;
	
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
		registerFields(fields,reflections);
		
		//remote values
		for(final Field field : fields){
			RemoteValue annotation = field.getAnnotation(RemoteValue.class);
			String key = annotation.key();
			Anchor anchor = field.getAnnotation(Anchor.class);
			if(anchor == null || anchor != null && anchor.anchor().equals(AnchorType.REMOTE)){
				client.watchLocal(key,new RemoteFieldChangeListener(field,this));
			}
		}
		
		//file values
		fields = reflections.getFieldsAnnotatedWith(FileValue.class);
		if(fields.size() != 0){
			fileWatcher = new FileWatcher(this);
			registerFields(fields,reflections);
			for(Field field : fields){
				String path_r = field.getAnnotation(FileValue.class).path();
				String path = Paths.get(path_r).toAbsolutePath().toString();
				String key = field.getAnnotation(FileValue.class).key();
				try {
					Anchor anchor = field.getAnnotation(Anchor.class);
					if(anchor == null || anchor != null && anchor.anchor().equals(AnchorType.FILE)){
						fileWatcher.registerWatcher(path, key, field);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		//define initial values
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
				fetchAndInjectNewValue(path + "@" + key, field, DataFrom.FILE);
			}
		}else if(df.from().equals(DataFrom.REMOTE)){
			RemoteValue rv = field.getAnnotation(RemoteValue.class);
			if(rv != null){
				String key = rv.key();
				fetchAndInjectNewValue(key, field, DataFrom.REMOTE);
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
			myself.injectValue(null,field);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}
	
	private void fetchAndInjectNewValue(final String key,final Field field,DataFrom from){
		if(from.equals(DataFrom.REMOTE)){
			YAFuture<YAEntry> f = client.get(key, YAMessage.Type.GET_LOCAL);
			f.addListener(new YAFutureListener(key){
	
				@Override
				public void operationCompleted(AbstractFuture<YAEntry> future) {
					if(future.isSuccess()){
						try {
							myself.injectValue(new String(future.get().getValue()),field);
						} catch (IllegalArgumentException | InterruptedException
								| ExecutionException e) {
							e.printStackTrace();
						}
						
						final FileValue fv = field.getAnnotation(FileValue.class);
						if(fv != null){
							Anchor anchor = field.getAnnotation(Anchor.class);
							if(anchor != null && anchor.anchor().equals(AnchorType.FILE)){
								return;
							}
							
							try {
								writeValueToFile(Paths.get(fv.path()).toAbsolutePath(),fv.key(),new String(future.get().getValue()));
							} catch (IllegalArgumentException | InterruptedException
									| ExecutionException e) {
								e.printStackTrace();
							}
						}
						
					}
				}
				
			});
		}else if(from.equals(DataFrom.FILE)){
			String value = readValueFromFile(key);
			injectValue(value,field);
			RemoteValue rv = field.getAnnotation(RemoteValue.class);
			if(rv != null){
				Anchor anchor = field.getAnnotation(Anchor.class);
				if(anchor != null && anchor.anchor().equals(AnchorType.REMOTE)){
					return;
				}
				client.put(rv.key(), value.getBytes(), YAMessage.Type.PUT_NOPROMISE);
			}
		}

	}

	private synchronized void writeValueToFile(Path path, String key,String value) {
		File thefile = path.toFile();
		OutputStreamWriter writer = null;
		InputStreamReader reader = null;
		BufferedReader bufferedReader = null;
		try {
			if(thefile.exists()){
				reader = new InputStreamReader(new FileInputStream(thefile));
				bufferedReader = new BufferedReader(reader);
				File tmpfile = new File(path + ".yatmp" + System.nanoTime());
				writer = new OutputStreamWriter(new FileOutputStream(tmpfile));
				
				String line = null;
				while((line = bufferedReader.readLine()) != null){
					if(line.contains("=")){
						String name = line.substring(0,line.lastIndexOf('='));
						if(name.equalsIgnoreCase(key)){
							writer.write(key + "=" + value + "\r");
						}else{
							writer.write(line + "\r");
						}
					}else{
						writer.write(line + "\r");
					}
				}
				writer.flush();
				reader.close();
				bufferedReader.close();
				writer.close();
				
				if(thefile.delete()){
					Files.copy(new File(tmpfile.getAbsolutePath()), new File(thefile.getAbsolutePath()));
					tmpfile.delete();
				}
			}else{
				thefile.createNewFile();
				writer = new OutputStreamWriter(new FileOutputStream(thefile));
				writer.write(key + "=" + value);
				writer.flush();
			}
		}catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(writer != null){
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(bufferedReader != null){
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
	
	private synchronized String readValueFromFile(String key) {
		String path = key.substring(0,key.lastIndexOf('@'));
		String filedName = key.substring(key.lastIndexOf('@') + 1);
		File file = Paths.get(path).toAbsolutePath().toFile();
		BufferedReader bufferedReader = null;
		InputStreamReader reader = null;
		System.out.println("read from file!!!!!!!!!!!!!!!!!");
		if(file.isFile() && file.exists()){
			try {
				reader = new InputStreamReader(new FileInputStream(file));
				bufferedReader = new BufferedReader(reader);
				String line = null;
				while((line = bufferedReader.readLine()) != null){
					if(line.contains("=")){
						String name = line.substring(0,line.lastIndexOf('='));
						String value = line.substring(line.lastIndexOf('=') + 1);
						if(name.equalsIgnoreCase(filedName)){
							return value == null ? "" : value;
						}
					}
					
					System.out.println(line);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				if(bufferedReader != null){
					try {
						bufferedReader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if(reader != null){
					try {
						reader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		return null;
	}

	public void register(AbstractConfig config) {
		synchronized(registry){
			registry.add(new SoftReference<AbstractConfig>(config,this.queue));
		}

		Field[] fs = config.getClass().getFields();
		for(Field f : fs){
			initSetValue(f);
		}
	}
	
	private void injectValue(String value,Field field) {
		if(Modifier.isStatic(field.getModifiers())){
			try {
				inject(field.getClass(),field,value);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
		} else {
			final Class<?> clazz = field.getDeclaringClass();
			
			synchronized(registry){
				for(SoftReference<AbstractConfig> config : registry){
					
					if(clazz.equals(config.get().getClass())){
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
}


