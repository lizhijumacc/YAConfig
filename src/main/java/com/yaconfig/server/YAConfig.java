package com.yaconfig.server;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;

import com.yaconfig.commands.Executor;
import com.yaconfig.commands.PutCommand;
import com.yaconfig.message.YAServerMessage;

import io.netty.channel.ChannelId;

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
	public static volatile AtomicLong VID;
	
	//the max ID of YAMessage which is already promised by Acceptors but not commit yet 
	public static volatile AtomicLong promisedNum;
	
	//the max ID of YAMessage which is wait for Acceptors promise
	//in a moment: VID < promisedNum < unpromisedNum
	public static volatile AtomicLong unpromisedNum;
	
	public YAConfigAcceptor acceptor;
	
	public YAConfigProposer proposer;
	
	public YAConfigServer server;
	
	public void init(int port) throws Exception {
		
		SERVER_ID = String.valueOf(port);
		
		exec = new Executor(this);
		
		ws = new Watchers();
		
		VID = new AtomicLong(0);
		promisedNum = new AtomicLong(-1);
		unpromisedNum = new AtomicLong(0);
		
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
			public void onChange(String key,byte[] value,int type) {
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
			public void onChange(String key, byte[] value,int type) {
				String masterId = new String(value);
				getEps().setCurrentMaster(masterId);
			}
			
		});
		ws.addWatcher(masterWatcher);
		
		
		HOST = InetAddress.getLocalHost().getHostAddress() + ":" 
					+ String.valueOf(port);
        

		acceptor = new YAConfigAcceptor(this);
		Thread acceptorThread = new Thread("acceptorThread"){
			@Override
			public void run(){
				acceptor.run();
			}
		};
		
		proposer = new YAConfigProposer(port,this);
		Thread proposerThread = new Thread("proposerThread"){
			@Override
			public void run(){
				try {
					proposer.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		if(port == 4247){
			server = new YAConfigServer(this);
			Thread serverThread = new Thread("proposerThread"){
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
		}
		
		proposerThread.start();
		Thread.sleep(2000);
		acceptorThread.start();
        getEps().run();
        
        
        Watcher test = new Watcher("com.test.test");
		test.setChangeListener(new IChangeListener(){

			@Override
			public void onChange(String key, byte[] value, int type) {
				
			}
			
		});
		ws.addWatcher(test);
        
	}

	public Watchers getWatcherSet() {
		return ws;
	}
	
	public void notifyWatchers(String key,byte[] value, int type){
		ws.notifyWatchers(key,value,type);
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

	public void broadcastToQuorums(YAServerMessage msg) {
		if(proposer != null){
			proposer.broadcastToQuorums(msg);
		}
	}

	public void redirectToMaster(YAServerMessage msg) {
		acceptor.redirectToMaster(msg);
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
			
			/*StringBuilder sb=new StringBuilder("");  
		    Exception e = new Exception("print stack");  
		      StackTraceElement[] trace = e.getStackTrace();;  
		      for (int i=0; i < trace.length; i++)  
		        sb.append("\tat " + trace[i]);  
		    
			
			System.out.println("I change to" + newStatus);
			System.out.println("who invoke me\n"+sb.toString());*/ 
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

	public long getUnpromisedNum() {
		return unpromisedNum.incrementAndGet();
	}

	public void setVID(String serverID, Long sequenceNum) {
		if(serverID == SERVER_ID){
			VID.set(sequenceNum);
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

	public void setChannelId(String ip, int port,ChannelId id) {
		eps.setChannelId(ip,port,id);
	}
	
}
