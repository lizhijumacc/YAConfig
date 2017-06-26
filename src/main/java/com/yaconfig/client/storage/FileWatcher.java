package com.yaconfig.client.storage;

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

import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.injector.FieldChangeCallback;

public class FileWatcher {
	FieldChangeCallback callback;
	Map<String,Field> watchers;
	Set<Path> pathSet;
	
	public FileWatcher(FieldChangeCallback callback){
		watchers = new HashMap<String,Field>();
		pathSet = new HashSet<Path>();
		this.callback = callback;
	}
	
	protected void notifyWatchers(String changedFile, Kind<?> kind) {
		Map<String,Field> fields = findFields(changedFile);
        if(kind.equals(StandardWatchEventKinds.ENTRY_MODIFY)){
           for(Entry<String,Field> entry : fields.entrySet()){
        	   callback.onUpdate(entry.getKey(), entry.getValue(),DataFrom.FILE);
           }
        }else if(kind.equals(StandardWatchEventKinds.ENTRY_CREATE)){
            for(Entry<String,Field> entry : fields.entrySet()){
         	   callback.onAdd(entry.getKey(), entry.getValue(),DataFrom.FILE);
            }
        }else if(kind.equals(StandardWatchEventKinds.ENTRY_DELETE)){
            for(Entry<String,Field> entry : fields.entrySet()){
           	   callback.onDelete(entry.getKey(), entry.getValue(),DataFrom.FILE);
            }
        }
	}

	private Map<String,Field> findFields(String changedFile) {
		HashMap<String,Field> returnMap = new HashMap<String,Field>();
		for(Entry<String,Field> entry : this.watchers.entrySet()){
			if(entry.getKey().startsWith(changedFile)){
				returnMap.put(entry.getKey(), entry.getValue());
			}
		}
		
		return returnMap;
	}

	public void registerWatcher(String file_s,String key,Field field) throws IOException{
		Path fpath = Paths.get(file_s).toAbsolutePath();
		final Path path = fpath.getParent();

		synchronized(watchers){
			watchers.put(fpath.toString() + "@" + key, field);
		}
		
		synchronized(pathSet){
			if(!pathSet.contains(path)){
				pathSet.add(path);
				ExecutorService watchTask = Executors.newSingleThreadExecutor();
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
				                   notifyWatchers(filePath,event.kind());
				                }

							}
						} catch (InterruptedException | IOException e) {
							e.printStackTrace();
						}
					}
					
				});
			}
		}
	}
}
