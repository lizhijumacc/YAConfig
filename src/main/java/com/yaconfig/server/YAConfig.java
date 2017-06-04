package com.yaconfig.server;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.yaconfig.commands.Executor;
import com.yaconfig.commands.PutCommand;
import com.yaconfig.message.YAMessage;

public class YAConfig{

	public static final int STATUS_REPORT_INTERVAL = 2000; //ms
	
	private EndPointSet eps;
	
	private Watchers ws;
	
	public Executor exec;
	
	public static String SERVER_ID;
	
	public String HOST;
	
	public volatile boolean IS_MASTER;
	
	public static volatile int STATUS = EndPoint.Status.INIT;
	
	public static volatile String SYSTEM_PERFIX = "system";
	
	public static Integer quorums;
	
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
		
		Watcher epWatcher = new Watcher(YAConfig.SYSTEM_PERFIX + ".node..*");
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
		
		Watcher masterWatcher = new Watcher(YAConfig.SYSTEM_PERFIX + ".master");
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
        
		
		//register self
        PutCommand put = new PutCommand("put");
        put.setExecutor(exec);
		put.execute((YAConfig.SYSTEM_PERFIX + ".node." + SERVER_ID),HOST.getBytes(),true);
	
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
        getEps().run();
        
        
        Watcher test = new Watcher("com.test.test");
		test.setChangeListener(new IChangeListener(){

			@Override
			public void onChange(String key, byte[] value) {
				
			}
			
		});
		ws.addWatcher(test);
        
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
        changeStatus.execute((YAConfig.SYSTEM_PERFIX + ".node." + SERVER_ID + ".status"),
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
		if(STATUS != newStatus){
			STATUS = newStatus;
			eps.changeMyselfStatus(newStatus);
			reportStatus();
		}
	}
	
	public boolean statusEquals(int status){
		return STATUS == status;
	}

	public static void dumpPackage(String string, Object msg) {
		if(!msg.toString().contains("status")){
			System.out.println(string + msg.toString());
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

	public boolean isSystemMessage(String key){
		if(key != null){
			return key.indexOf(YAConfig.SYSTEM_PERFIX) == 0;
		}
		return false;
	}

	public void peerDead(String ip, String port) {
		eps.peerDead(ip,port);
	}
	
}
