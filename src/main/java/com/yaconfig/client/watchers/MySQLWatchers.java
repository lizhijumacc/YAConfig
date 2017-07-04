package com.yaconfig.client.watchers;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.reflections.Reflections;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.yaconfig.client.Constants;
import com.yaconfig.client.annotation.Anchor;
import com.yaconfig.client.annotation.MySQLValue;
import com.yaconfig.client.injector.AnchorType;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.injector.FieldChangeCallback;
import com.yaconfig.client.injector.FieldChangeListener;
import com.yaconfig.client.Constants;

@WatchersType(from = Constants.fromMySQL)
public class MySQLWatchers extends AbstractWatchers {
	
	MySQLWatchers myself;
	HashMap<String,BinaryLogClient> connections = new HashMap<String,BinaryLogClient>();
	HashMap<BinaryLogClient,ExecutorService> services = new HashMap<BinaryLogClient,ExecutorService>();
	
	public MySQLWatchers(){
		
	}
	
	public MySQLWatchers(Reflections rfs, FieldChangeCallback callback){
		this.init(rfs, callback);
	}
	
	@Override
	public Set<Field> init(Reflections rfs, FieldChangeCallback callback) {
		myself = this;
		Set<Field> fields = rfs.getFieldsAnnotatedWith(MySQLValue.class);
		for(final Field field : fields){
			MySQLValue annotation = field.getAnnotation(MySQLValue.class);
			String tableName = annotation.tableName();
			String key = annotation.key();
			String connStr = annotation.connStr();
			Anchor anchor = field.getAnnotation(Anchor.class);
			if(anchor == null || anchor != null && anchor.anchor().equals(AnchorType.MYSQL)){
				watch(connStr + Constants.CONNECTION_KEY_SEPERATOR + tableName + Constants.CONNECTION_KEY_SEPERATOR + key,new FieldChangeListener(field,callback));
			}
		}
		
		return fields;
	}

	private void addConnection(final String connStr,final String tableName,String username,String password) {
		String hostName = getHostName(connStr);
		int port = getPort(connStr);
		final String database = getDataBase(connStr);
		String connId = hostName + port;
		if(connections.containsKey(connId)){
			return;
		}
		final BinaryLogClient client = new BinaryLogClient(hostName, port, username, password);
		
		client.registerEventListener(new EventListener() {

			private long tableId = -1;
			
		    @Override
		    public void onEvent(Event event) {
		    	//binlog should be type = row RBR
		    	EventHeader eventh = event.getHeader();
		    	EventData eventd = event.getData();
		    	
		    	if(eventh.getEventType().equals(EventType.TABLE_MAP)){
		    		TableMapEventData eventdd = (TableMapEventData)eventd;
		    		String tn = eventdd.getTable();
		    		String db = eventdd.getDatabase();
		    		long tid = eventdd.getTableId();
		    		if(tn.equalsIgnoreCase(tableName) && database.equalsIgnoreCase(db)){
		    			tableId = tid;
		    		}else{
		    			tableId = -1;
		    		}
		    		
		    		return;
		    	}
		    	
		    	YAEventType yaeventType = null;
		    	List<Serializable[]> rows = null;
		    	
		    	if(EventType.isDelete(eventh.getEventType())){
		    		DeleteRowsEventData eventdd = (DeleteRowsEventData)eventd;
		    		if(eventdd.getTableId() != tableId){
		    			return;
		    		}
		    		yaeventType = YAEventType.DELETE;
		    		rows = eventdd.getRows();
		    	}
		    	
		    	if(EventType.isUpdate(eventh.getEventType())){
		    		UpdateRowsEventData eventdd = (UpdateRowsEventData)eventd;
		    		if(eventdd.getTableId() != tableId){
		    			return;
		    		}
		    		yaeventType = YAEventType.UPDATE;
		    		List<Map.Entry<Serializable[], Serializable[]>> ss = eventdd.getRows();
		    		rows = new ArrayList<Serializable[]>();
		    		for(Map.Entry<Serializable[], Serializable[]> sss : ss){
		    			rows.add(sss.getKey());
		    		}
		    	}
		    	
		    	if(EventType.isWrite(eventh.getEventType())){
		    		WriteRowsEventData eventdd = (WriteRowsEventData)eventd;
		    		if(eventdd.getTableId() != tableId){
		    			return;
		    		}
		    		yaeventType = YAEventType.ADD;
		    		rows = eventdd.getRows();
		    	}
		    
		    	if(rows == null || yaeventType == null){
		    		return;
		    	}
	            for (Object[] row : rows) {
	                String row_s = Arrays.toString(row);
	                Watcher w = myself.findWatcher(connStr,tableName,row_s);
	                if(w != null){
	                	notifyWatchers(w.getKey(), yaeventType, DataFrom.MYSQL);
	                }
	            }
		    }
		    
		});
		ExecutorService s = Executors.newSingleThreadExecutor();
		s.execute(new Runnable(){

			@Override
			public void run() {
				try {
					client.connect();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		});
		services.put(client, s);
		connections.put(connId, client);
	}

	protected Watcher findWatcher(String connStr, String tableName, String row_s) {
		for(Watcher w : watchers.values()){
			String tn = getTableName(w.getKey());
			String fn = getFieldName(w.getKey());
			if(w.getKey().startsWith(connStr) && tableName.equals(tn)){
				if(row_s.contains(fn)){
					return w;
				}
			}
		}
		return null;
	}

	private String getDataBase(String connStr) {
		String s = connStr.substring(connStr.lastIndexOf("/") + 1);
		if(!connStr.contains("?")){
			return s;
		}else{
			return s.substring(0,s.lastIndexOf("?"));
		}
	}

	private String getTableName(String location) {
		String[] s = location.split("\\" + Constants.CONNECTION_KEY_SEPERATOR);
		return s[1];
	}
	
	private String getFieldName(String location) {
		String[] s = location.split("\\" + Constants.CONNECTION_KEY_SEPERATOR);
		return s[2];
	}

	private int getPort(String connStr) {
		String portstr = connStr.substring(connStr.lastIndexOf(":") + 1,connStr.lastIndexOf("/"));
		return Integer.parseInt(portstr);
	}

	private String getHostName(String connStr) {
		return connStr.substring(connStr.indexOf("//") + 2,connStr.lastIndexOf(":"));
	}

	@Override
	public void watch(String key, WatcherListener... listeners) {
		watchLocal(key, listeners);
		
		String hostName = getHostName(key);
		int port = getPort(key);
		
		if(connections.get(hostName + port) == null){
			Field field = ((FieldChangeListener)listeners[0]).getField();
			MySQLValue annotation = field.getAnnotation(MySQLValue.class);
			String connStr = annotation.connStr();
			String userName = annotation.userName();
			String password = annotation.password();
			String tableName = annotation.tableName();
			addConnection(connStr,tableName,userName,password);
		}
	}

	@Override
	public void unwatch(String key, WatcherListener... listeners) {
		unwatchLocal(key,listeners);
		String hostName = getHostName(key);
		int port = getPort(key);
		boolean needRemove = true;
		for(Watcher w : watchers.values()){
			if(w.getKey().contains(hostName + ":" + port)){
				needRemove = false;
				break;
			}
		}
		BinaryLogClient blc = connections.get(hostName + port);
		if(needRemove && blc != null){
			try {
				blc.disconnect();
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				services.get(blc).shutdownNow();
				services.remove(blc);
				connections.remove(hostName + port);
			}
		}
	}

}
