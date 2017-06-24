package com.yaconfig.client;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
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
import com.yaconfig.client.message.YAMessage;

public class PackageScanner {

	private YAConfigClient yaconfig;
	
	public PackageScanner(YAConfigClient yaconfig){
		this.yaconfig = yaconfig;
	}
	
	public void scan(String[] packageList) {
		Reflections reflections = getReflection(packageList);
		
		Set<Field> fields = reflections.getFieldsAnnotatedWith(YAConfigValue.class);
		
		for(final Field field : fields){
			YAConfigValue annotation = field.getAnnotation(YAConfigValue.class);
			String key = annotation.key();
			YAFuture<YAEntry> future = (YAFuture<YAEntry>) yaconfig.watch(key, new WatcherListener(){

				public void onDelete(Watcher w, String key) {
					try {
						System.out.println("scan delete.");
						yaconfig.injectValue(new YAEntry(key,null),field);
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					}
				}

				public void onAdd(Watcher w, String key) {
					System.out.println("scan add.");
					YAFuture<YAEntry> f = yaconfig.get(key, YAMessage.Type.GET_LOCAL);
					if(f.isSuccess()){
						try {
							yaconfig.injectValue(f.get(),field);
						} catch (IllegalArgumentException | InterruptedException
								| ExecutionException e) {
							e.printStackTrace();
						}
					}
				}

				public void onUpdate(Watcher w, String key) {
					System.out.println("scan update.");
					YAFuture<YAEntry> f = (YAFuture<YAEntry>) yaconfig.get(key, YAMessage.Type.GET_LOCAL).awaitUninterruptibly();
					if(f.isSuccess()){
						try {
							yaconfig.injectValue(f.get(),field);
						} catch (IllegalArgumentException | InterruptedException
								| ExecutionException e) {
							e.printStackTrace();
						}
					}
				}
				
			}).awaitUninterruptibly();
			
			if(!future.isSuccess()){
				System.out.println("Watch KEY:" + key + " ERROR in scanning.");
				try {
					future.get();
				} catch (InterruptedException | ExecutionException e) {
					System.out.println(e.getCause().getMessage());
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
	
}
