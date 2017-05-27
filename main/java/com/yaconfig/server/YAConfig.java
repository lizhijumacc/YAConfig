package com.yaconfig.server;

import java.net.InetAddress;
import java.util.Iterator;

import com.yaconfig.commands.Executor;
import com.yaconfig.commands.PutCallback;
import com.yaconfig.commands.PutCommand;

public class YAConfig{

	protected static final long HEARTBEAT_INTVAL = 2000; //ms

	private static EndPointSet eps;
	
	private static Watchers ws = new Watchers();
	
	public static Executor exec = new Executor();
	
	public static volatile String SERVER_ID;
	
	public static String HOST;
	
	public static volatile boolean IS_MASTER;
	
	public static volatile EndPoint.EndPointStatus STATUS = EndPoint.EndPointStatus.INIT;
	
	public static Integer quorums;
	
	private static Thread heartbeat;
	
	public static volatile int VID;
	
	public static YAConfigClient client;
	
	public static YAConfigServer server;
	
	public static void init(int port) throws Exception {
		
		SERVER_ID = String.valueOf(port);
		
		IS_MASTER = false;
		
		//should read from local config file.
		quorums = 3;
        
		eps = new EndPointSet();
		eps.add(new EndPoint("4241","127.0.0.1:4241"));
		eps.add(new EndPoint("4242","127.0.0.1:4242"));
		eps.add(new EndPoint("4243","127.0.0.1:4243"));
		//eps.add(new EndPoint("4244","127.0.0.1:4244"));
		
		Watcher epWatcher = new Watcher("com.yaconfig.node..*");
		epWatcher.setChangeListener(new IChangeListener(){

			@Override
			public void onChange(String key,byte[] value) {
				String suffix = key.substring(key.lastIndexOf(".") + 1);
				
				switch (suffix) {
				case "status":
				    getEps().setEPStatus(key,new String(value));
				    break;
				case "vote":
					getEps().setVotes(key,new String(value));
				}
			}
			
		});
		ws.addWatcher(epWatcher);
		
		Watcher masterWatcher = new Watcher("com.yaconfig.master");
		masterWatcher.setChangeListener(new IChangeListener(){

			@Override
			public void onChange(String key, byte[] value) {
				String masterId = new String(value);
				getEps().setCurrentMaster(masterId);
			}
			
		});
		ws.addWatcher(masterWatcher);
		
		
		HOST = InetAddress.getLocalHost().getHostAddress() + ":" 
					+ String.valueOf(port);
        
        //start heartbeat
        heartbeat = new Thread(){
			@Override
			public void run(){
				for(;;){
			        reportStatus();
			        try {
						Thread.sleep(HEARTBEAT_INTVAL);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		//register self
        PutCommand put = new PutCommand("put");
        put.setExecutor(exec);
		put.setCallback(new PutCallback(){
			
			@Override
			public void callback(){
				heartbeat.start();
				getEps().startMasterListening();
			}
		});
		put.execute(("com.yaconfig.node." + SERVER_ID),HOST.getBytes());
	
		client = new YAConfigClient();
		Thread clientThread = new Thread(){
			@Override
			public void run(){
				client.run();
			}
		};
		
		server = new YAConfigServer(port);
		Thread serverThread = new Thread(){
			@Override
			public void run(){
				try {
					server.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		
		serverThread.start();
		Thread.sleep(2000);
		clientThread.start();
	}

	public Watchers getWatcherSet() {
		return ws;
	}
	
	public static void notifyWatchers(String key,byte[] value){
		ws.notifyWatchers(key,value);
	}

	public static void reportStatus() {
        PutCommand changeStatus = new PutCommand("put");
        changeStatus.setExecutor(YAConfig.exec);
        changeStatus.execute(("com.yaconfig.node." + YAConfig.SERVER_ID + ".status"),
        		YAConfig.STATUS.toString().getBytes());
	}

	public static EndPointSet getEps() {
		return eps;
	}

	public static void setEps(EndPointSet eps) {
		YAConfig.eps = eps;
	}

	public static void broadcastToQuorums(YAMessage msg) {
		if(server != null){
			server.broadcastToQuorums(msg);
		}
	}

	public static void redirectToMaster(YAMessage msg) {
		YAConfig.sendMessage(eps.currentMaster,msg);
	}

	private static void sendMessage(EndPoint currentMaster, YAMessage msg) {
		client.sendMessage(currentMaster,msg);
	}

	public static void processMessage(YAMessage yamsg) {
		if(yamsg.type == YAMessage.Type.PUT){
			PutCommand put = new PutCommand("put");
			put.setExecutor(exec);
			put.execute(yamsg);
		}else if(yamsg.type == YAMessage.Type.GET){
			//TODO
		}
	}
	
    public static void main( String[] args ) throws Exception{
        int port;
        
        if (args.length > 0){
        	port = Integer.parseInt(args[0]);
        }else{
        	port = 4242;
        }
        
        YAConfig.init(port);
    }
}
