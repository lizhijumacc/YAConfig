package com.yaconfig.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.yaconfig.commands.GetCommand;
import com.yaconfig.commands.PutCallback;
import com.yaconfig.commands.PutCommand;

public class EndPointSet {
	public ConcurrentHashMap<String, EndPoint> eps;
	
	public volatile EndPoint currentMaster;
	
	public volatile EndPoint nextMaster;
	
	private Thread masterElection;
	
	private Integer resolutionThreshold;
	
	public EndPointSet(){
		eps = new ConcurrentHashMap<String,EndPoint>();
		resolutionThreshold = (YAConfig.quorums / 2) + 1;
	}
	
	//only add from config file.
	public void add(EndPoint e){
		if(!eps.contains(e)){
			eps.put(e.host(), e);
		}
	}

	public void setEPStatus(String key, String status) {
		GetCommand get = new GetCommand("get");
		get.setExecutor(YAConfig.exec);
		byte[] result = get.executeQuery(key.substring(0, key.lastIndexOf(".")));
		if(result != null){
			String res = new String(result);
			EndPoint dummy = new EndPoint(key.substring(key.lastIndexOf(".")),res);
			if(eps.contains(dummy)){
				synchronized(eps){
					EndPoint ep = eps.get(dummy.host());
					ep.status = EndPoint.EndPointStatus.valueOf(status);
					ep.heartbeatTimestamp = System.currentTimeMillis();
				}
			}
		}
	}

	public void startElection() {
		YAConfig.STATUS = EndPoint.EndPointStatus.ELECTING;
		YAConfig.reportStatus();
		masterElection = new Thread(){
			@Override
			public void run(){
				
				for(;;){
					//check endpoint is alive or not.
					synchronized(eps){
						if(hasEnoughAliveEP()){
							for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
								String key = it.next();
								EndPoint ep = (EndPoint)eps.get(key);
								
								//if there is no master in system.
								GetCommand gc = new GetCommand("get");
								gc.setExecutor(YAConfig.exec);

							    String s = String.valueOf(gc.executeQuery("com.yaconfig.master"));
								if(s == null){
									YAConfig.STATUS = EndPoint.EndPointStatus.ELECTING;
									YAConfig.reportStatus();
									voteNextMaster();
								}
								
								//if master's heartbeat is timeout
								if(System.currentTimeMillis() - ep.heartbeatTimestamp 
										> 2 * YAConfig.HEARTBEAT_INTVAL){
									ep.status = EndPoint.EndPointStatus.DEAD;
									if(ep.equals(currentMaster)){
										reElection();
									}
									//if next master which I am already voted is dead during election period
									//get new next master and vote again
									//if lack this re-vote process,the master election resolution may never reached
									else if(ep.equals(nextMaster) 
											&& YAConfig.STATUS == EndPoint.EndPointStatus.ELECTING){
										voteNextMaster();
									}
								}
							}
						}else if(YAConfig.STATUS == EndPoint.EndPointStatus.LEADING){
							YAConfig.STATUS = EndPoint.EndPointStatus.ELECTING;
							YAConfig.reportStatus();
						}
					}
					
					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

			private boolean hasEnoughAliveEP() {
				Integer countDead = 0;
				for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
					String key = it.next();
					EndPoint ep = (EndPoint)eps.get(key);
					if(ep.status == EndPoint.EndPointStatus.DEAD){
						countDead++;
					}
				}
				
				return countDead < resolutionThreshold;
			}
		};
		
		masterElection.start();
	}

	protected void reElection() {
		//proposal the node as master which has minimum SERVER_ID
		voteNextMaster();
		

        //start status barrier to wait all the other endpoint status change to ELECTING
		for(;;){
			HashSet<EndPoint> notwatingeps = new HashSet<EndPoint>();
			int deadCount = 0;
			for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
				String key = it.next();
				EndPoint ep = (EndPoint)eps.get(key);
				if(ep.status != EndPoint.EndPointStatus.DEAD || 
						ep.status != EndPoint.EndPointStatus.ELECTING){
					notwatingeps.add(ep);
				}
				
				if(ep.status == EndPoint.EndPointStatus.DEAD || 
						ep.status == EndPoint.EndPointStatus.UNKOWN){
					deadCount++;
				}
			}
			
			//wait for enough endpoint online
			if(deadCount >= resolutionThreshold){
				continue;
			}
			
	        //if the old leader is fake dead(because of networking jitter) in this waiting period,
	        //the condition may never be achieved,so break the forever loop when old leader online again.
			if(notwatingeps.contains(currentMaster)
					&& currentMaster.status == EndPoint.EndPointStatus.LEADING
					&& !YAConfig.IS_MASTER){
				YAConfig.STATUS = EndPoint.EndPointStatus.FOLLOWING;
				YAConfig.reportStatus();
				break;
			}
			
			//if all the endpoint is waiting for new leader
			if(notwatingeps.size() == 0){
				//count votes & change status to LEADING or FOLLWING
				while(countVotes()){}
				break;
			}
		}
		
	}

	private void voteNextMaster() {
		nextMaster = getNextMaster();
        PutCommand voteNextMaster = new PutCommand("put");
        voteNextMaster.setExecutor(YAConfig.exec);
        voteNextMaster.execute(("com.yaconfig.node." + YAConfig.SERVER_ID + ".vote"),
        		nextMaster.host().getBytes());
        
        voteNextMaster.callback = new PutCallback(){

			@Override
			public void callback() {
				YAConfig.STATUS = EndPoint.EndPointStatus.ELECTING;
				YAConfig.reportStatus();	
			}
        	
        };
	}

	private boolean countVotes() {
		
		HashMap<String,Integer> voteBox = new HashMap<String,Integer>();
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = (EndPoint)eps.get(key);
			if(voteBox.get(ep.voteMaster) != null){
				Integer count = voteBox.get(ep.voteMaster);
				voteBox.put(ep.voteMaster, ++count);
			}else{
				voteBox.put(ep.voteMaster, 1);
			}
		}		
		
		for(Iterator<String> it = voteBox.keySet().iterator();it.hasNext();){
			String key = it.next();
			Integer count = voteBox.get(key);
			
			if(count >= resolutionThreshold){
				//set master only if I am master,
				//other endpoint should be notified by "com.yaconfig.master" value
				setMaster(key);
				return false;
			}
		}
		
		return true;
	}

	private void setMaster(String key) {
		EndPoint dummy = new EndPoint("master",key);
		EndPoint myself = new EndPoint("myself",YAConfig.HOST);
		if(dummy.equals(myself)){
	        PutCommand setMaster = new PutCommand("put");
	        setMaster.setExecutor(YAConfig.exec);
	        setMaster.execute("com.yaconfig.master",YAConfig.HOST.getBytes());
	        
	        setMaster.callback = new PutCallback(){
	
				@Override
				public void callback() {
					YAConfig.IS_MASTER = true;
					YAConfig.STATUS = EndPoint.EndPointStatus.LEADING;
					YAConfig.reportStatus();
				}
	        	
	        };
		}
	}

	private EndPoint getNextMaster() {
		int minId = Integer.MAX_VALUE;
		int maxVID = Integer.MIN_VALUE;
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = (EndPoint)eps.get(key);
			int sid = Integer.parseInt(ep.getServerId());
			if(sid < minId && ep.VID > maxVID
					&& ep.status == EndPoint.EndPointStatus.ELECTING 
					 	|| ep.status == EndPoint.EndPointStatus.LEADING
					 	|| ep.status == EndPoint.EndPointStatus.FOLLOWING){
				minId = sid;
			}
		}
		
		return eps.get(String.valueOf(minId));
	}

	public void setVotes(String key, String string) {
		eps.get(key).voteMaster = string;
	}

	private class WatingMasterLeading implements Callable<Boolean>{

		private String masterHost;
		
		public WatingMasterLeading(String masterHost){
			this.masterHost = masterHost;
		}
		
		@Override
		public Boolean call() {
			for(;;){
				if(YAConfig.getEps().isLeading(masterHost)){
					YAConfig.getEps().currentMaster = new EndPoint("master",masterHost);
					YAConfig.STATUS = EndPoint.EndPointStatus.FOLLOWING;
					YAConfig.reportStatus();
					return true;
				}
			}			
		}
		
	}
	
	public void setCurrentMaster(String masterHost) {
		//waiting for current master begin to lead.
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Boolean> future = executor.submit(new WatingMasterLeading(masterHost));

		try{
			future.get(2 * YAConfig.HEARTBEAT_INTVAL, TimeUnit.MILLISECONDS);
		}catch (InterruptedException e) {  
            System.out.println("waiting master leading has been interrupted.");  
            future.cancel(true);     
        } catch (ExecutionException e) {     
        	System.out.println("waiting master leading thread server error.");
            future.cancel(true); 
        } catch (TimeoutException e) {
        	//if and only if master dead during notify all other endpoint
        	//leading will timeout
        	reElection();
            System.out.println("waiting master leading timeout");     
            future.cancel(true);  
        }finally{
			executor.shutdown();
		}
	
	}

	private boolean isLeading(String masterHost) {
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = eps.get(key);	
			if(ep.host().equals(masterHost)){
				return ep.status == EndPoint.EndPointStatus.LEADING;
			}
		}
		
		return false;
	}
}
