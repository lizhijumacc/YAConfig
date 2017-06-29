package com.yaconfig.client.watchers;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.yaconfig.client.Constants;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.injector.FieldChangeCallback;

public class FileWatchers extends AbstractWatchers {
	HashMap<Path,ExecutorService> pathSet;
	HashMap<Path,Set<Watcher>> pathMapWatchers;
	
	public FileWatchers(FieldChangeCallback callback){
		pathSet = new HashMap<Path,ExecutorService>();
		pathMapWatchers = new HashMap<Path,Set<Watcher>>();
	}
	
	protected void notifyFileWatchers(String changedFile, Kind<?> kind) {
		Map<String,Watcher> watchers = findWatchers(changedFile);
        if(kind.equals(StandardWatchEventKinds.ENTRY_MODIFY)){
           for(Entry<String,Watcher> entry : watchers.entrySet()){
        	   super.notifyWatchers(entry.getKey(), EventType.UPDATE, DataFrom.FILE);
           }
        }else if(kind.equals(StandardWatchEventKinds.ENTRY_CREATE)){
            for(Entry<String,Watcher> entry : watchers.entrySet()){
            	super.notifyWatchers(entry.getKey(), EventType.ADD, DataFrom.FILE);
            }
        }else if(kind.equals(StandardWatchEventKinds.ENTRY_DELETE)){
            for(Entry<String,Watcher> entry : watchers.entrySet()){
            	super.notifyWatchers(entry.getKey(), EventType.DELETE, DataFrom.FILE);
            }
        }
	}

	private Map<String,Watcher> findWatchers(String changedFile) {
		HashMap<String,Watcher> returnMap = new HashMap<String,Watcher>();
		for(Entry<String,Watcher> entry : this.watchers.entrySet()){
			if(entry.getKey().startsWith(changedFile)){
				returnMap.put(entry.getKey(), entry.getValue());
			}
		}
		
		return returnMap;
	}

	@Override
	public void watch(String key,WatcherListener... listeners){

		boolean needRegister = watchLocal(key, listeners);
		
		if(!needRegister){
			return;
		}
		
		String file = key.substring(0,key.indexOf(Constants.FILE_KEY_SEPERATOR));
		final Path path = Paths.get(file.substring(0,file.lastIndexOf(File.separator)));
		
		synchronized(pathSet){
			if(!pathSet.containsKey(path)){
				ExecutorService watchTask = Executors.newSingleThreadExecutor();
				pathSet.put(path, watchTask);
				pathMapWatchers.put(path, new HashSet<Watcher>());
				pathMapWatchers.get(path).add(getWatcher(key));
				watchTask.execute(new Runnable(){

					@Override
					public void run() {
						try {
							while(true){
								WatchService watchService = FileSystems.getDefault().newWatchService();
								path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY ,  
								        StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE );
								WatchKey wk = watchService.take();
								
								for (WatchEvent<?> event : wk.pollEvents()) {  
								   String filePath = path.toString() + File.separator + (Path)event.context();
				                   notifyFileWatchers(filePath,event.kind());
				                }

							}
						} catch (InterruptedException | IOException e) {
							e.printStackTrace();
						}
					}
					
				});
			}else{
				pathMapWatchers.get(path).add(getWatcher(key));
			}
		}
	}
	
	@Override
	public void unwatch(String key,WatcherListener... listeners){
		boolean needUnregister = unwatchLocal(key,listeners);
		if(needUnregister){
			String file = key.substring(0,key.indexOf(Constants.FILE_KEY_SEPERATOR));
			final Path path = Paths.get(file.substring(0,file.lastIndexOf(File.separator)));
			pathMapWatchers.get(path).remove(getWatcher(key));
			if(pathMapWatchers.get(path).size() == 0){
				pathSet.get(path).shutdownNow();
				pathSet.remove(path);
			}
		}
	}
}
