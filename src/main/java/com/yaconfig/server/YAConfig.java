package com.yaconfig.server;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.yaconfig.commands.Executor;
import com.yaconfig.commands.PutCommand;

public class YAConfig{

	protected static final long HEARTBEAT_INTVAL = 2000; //ms

	private EndPointSet eps;
	
	private Watchers ws;
	
	public Executor exec;
	
	public static String SERVER_ID;
	
	public String HOST;
	
	public volatile boolean IS_MASTER;
	
	private volatile int STATUS = EndPoint.Status.INIT;
	
	private static final AtomicIntegerFieldUpdater<YAConfig> STATUS_UPDATER = 
			AtomicIntegerFieldUpdater.newUpdater(YAConfig.class, "STATUS");
	
	public static Integer quorums;
	
	private Thread heartbeat;
	
	//the max ID of YAMessage which is already commit 
	public static volatile long VID;
	
	//the max ID of YAMessage which is already promised by Acceptors but not commit yet 
	public static volatile long promisedNum;
	
	//the max ID of YAMessage which is wait for Acceptors promise
	//in a moment: VID < promisedNum < unpromisedNum
	public static volatile long unpromisedNum;
	
	public YAConfigClient client;
	
	public YAConfigServer server;
	
	public void init(int port) throws Exception {
		
		SERVER_ID = String.valueOf(port);
		
		exec = new Executor(this);
		
		ws = new Watchers();
		
		VID = 0;
		promisedNum = -1;
		unpromisedNum = 0;
		
		IS_MASTER = false;
		
		//should read from local config file.
		quorums = 3;
        
		eps = new EndPointSet(this);
		eps.add(new EndPoint("4247","127.0.0.1:4247"));
		eps.add(new EndPoint("4248","127.0.0.1:4248"));
		eps.add(new EndPoint("4249","127.0.0.1:4249"));
		//eps.add(new EndPoint("4244","127.0.0.1:4244"));
		
		Watcher epWatcher = new Watcher("com.yaconfig.node..*");
		epWatcher.setChangeListener(new IChangeListener(){

			@Override
			public void onChange(String key,byte[] value) {
				String suffix = key.substring(key.lastIndexOf(".") + 1);
				
				switch (suffix) {
				case "status":
				    getEps().setEPStatus(key,Integer.parseInt(new String(value)));
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
        heartbeat = new Thread("heartbeatThread"){
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
		put.execute(("com.yaconfig.node." + SERVER_ID),HOST.getBytes(),true);
	
		client = new YAConfigClient(this);
		Thread clientThread = new Thread("clientThread"){
			@Override
			public void run(){
				client.run();
			}
		};
		
		server = new YAConfigServer(port,this);
		Thread serverThread = new Thread("serverThread"){
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
		Thread.sleep(1000);
        heartbeat.start();
        getEps().run();
        
  
        Watcher test = new Watcher("com.yaconfig.test");
		test.setChangeListener(new IChangeListener(){

			@Override
			public void onChange(String key, byte[] value) {
				System.out.println(new String(value));
			}
			
		});
		ws.addWatcher(test);
        
        while(true){
        	if(IS_MASTER){
        		Thread.sleep(5000);
	        	for(int i=0;i<10000000;i++){
	        		PutCommand p = new PutCommand("put");
	        		p.setExecutor(exec);
	        		p.execute("com.yaconfig.test", "testvalue".getBytes(), false);
	        		Thread.sleep(10);
	        	}
        	}
        }
        

	}

	public Watchers getWatcherSet() {
		return ws;
	}
	
	public void notifyWatchers(String key,byte[] value){
		ws.notifyWatchers(key,value);
	}

	public void reportStatus() {
        PutCommand changeStatus = new PutCommand("put");
        changeStatus.setExecutor(exec);
        changeStatus.execute(("com.yaconfig.node." + SERVER_ID + ".status"),
        		String.valueOf(STATUS).getBytes(),true);
	}

	public EndPointSet getEps() {
		return eps;
	}

	public void setEps(EndPointSet eps) {
		this.eps = eps;
	}

	public void broadcastToQuorums(YAMessage msg) {
		if(server != null){
			server.broadcastToQuorums(msg);
		}
	}

	public void redirectToMaster(YAMessage msg) {
		client.redirectToMaster(msg);
	}
	
    public static void main( String[] args ) throws Exception{
        int port;
        
        if (args.length > 0){
        	port = Integer.parseInt(args[0]);
        }else{
        	port = 4247;
        }
        
        YAConfig yaconfig = new YAConfig();
        yaconfig.init(port);
    }
    
	public static void printImportant(String head,String info){
    	synchronized(System.out){
    		System.out.println();
    		System.out.println("+--------------------"+ head +"-----------------+");
    		System.out.println("|------"+ info +"-----------------|");
    		System.out.println("+-----------------------------------------------+");
    		System.out.println();
    	}
    }
	
	public void changeStatus(int newStatus) {
		for(;;){
			int oldStatus = STATUS_UPDATER.get(this);
			if(STATUS_UPDATER.compareAndSet(this, oldStatus, newStatus)){
				break;
			}
		}
		reportStatus();
	}
	
	public boolean statusEquals(int status){
		int myStatus = STATUS_UPDATER.get(this);
		return myStatus == status;
	}

	public static void dumpPackage(String string, Object msg) {
		if(!msg.toString().contains("status")){
			//System.out.println(string + msg.toString());
		}
	}

	public synchronized long getUnpromisedNum() {
		unpromisedNum++;
		return unpromisedNum;
	}

	public void setVID(String serverID, Long sequenceNum) {
		if(serverID == SERVER_ID){
			VID = sequenceNum;
		}
		eps.setVID(serverID,sequenceNum);
	}
}
