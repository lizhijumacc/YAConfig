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

import com.yaconfig.commands.PutCommand;

public class EndPointSet {
	public ConcurrentHashMap<String, EndPoint> eps;
	
	public volatile EndPoint currentMaster;
	
	public volatile EndPoint nextMaster;
	
	private Thread masterListeningThread;
	
	private Thread masterElectingThread;
	
	private Thread countVotesThread;
	
	private volatile boolean electing = false;
	
	private volatile boolean counting = false;
	
	private Integer resolutionThreshold;
	
	public EndPointSet(){
		eps = new ConcurrentHashMap<String,EndPoint>();
		resolutionThreshold = (YAConfig.quorums / 2) + 1;
	}
	
	//only add from config file.
	public void add(EndPoint e){
		if(!eps.contains(e)){
			eps.put(e.getServerId(), e);
		}
	}

	public void setEPStatus(String key, String status) {
		EndPoint ep = eps.get(getServerIdFromKey(key));
		if(null != ep){
			ep.status = EndPoint.EndPointStatus.valueOf(status);
			ep.heartbeatTimestamp = System.currentTimeMillis();
		}
	}

	private String getServerIdFromKey(String key) {
		String epkey = key.substring(0, key.lastIndexOf("."));
		return epkey.substring(epkey.lastIndexOf(".") + 1);
	}

	public void startMasterListening() {
		masterListeningThread = new Thread(){
			@Override
			public void run(){
				
				for(;;){
					//check endpoint is alive or not.
					synchronized(eps){
						if(hasEnoughAliveEP()){
							for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
								String key = it.next();
								EndPoint ep = (EndPoint)eps.get(key);
								
								//if there is no master in system or master conflict
								if(needElectMaster()){
									YAConfig.STATUS = EndPoint.EndPointStatus.ELECTING;
									voteNextMaster();
								}
								
								//if master's heartbeat is timeout
								if(System.currentTimeMillis() - ep.heartbeatTimestamp 
										> 2 * YAConfig.HEARTBEAT_INTVAL){
									ep.status = EndPoint.EndPointStatus.DEAD;
									if(ep.equals(currentMaster)){
										electing = true;
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
		
		
		masterElectingThread = new Thread(){
			@Override 
			public void run(){
				while(true){
					reElection();
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		countVotesThread = new Thread(){
			@Override
			public void run(){
				while(true){
					try {
						while(counting){
							countVotes();
							Thread.sleep(200);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		//wait for 2 heartbeat interval to get other endpoint status
		//then start master listening.
		try {
			Thread.sleep(2 * YAConfig.HEARTBEAT_INTVAL);
			masterListeningThread.start();
			masterElectingThread.start();
			countVotesThread.start();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	protected boolean needElectMaster() {
		int leaderCount = 0;
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = (EndPoint)eps.get(key);
			if(ep.status == EndPoint.EndPointStatus.LEADING){
				leaderCount ++;
			}
		}
		return leaderCount != 1;
	}

	protected void reElection() {
        //start status barrier to wait all the other endpoint status change to ELECTING
		while(electing){
			//proposal the node as master which has minimum SERVER_ID
			voteNextMaster();
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
			
			
	        //if the old leader is fake dead(because of networking jitter) in this waiting period,
	        //the condition may never be achieved,so break the forever loop when old leader online again.
			if(notwatingeps.contains(currentMaster)
					&& currentMaster.status == EndPoint.EndPointStatus.LEADING
					&& !YAConfig.IS_MASTER){
				YAConfig.STATUS = EndPoint.EndPointStatus.FOLLOWING;
				YAConfig.reportStatus();
				electing = false;
			}
			
			//if all the endpoint is waiting for new leader
			if(notwatingeps.size() == 0 && deadCount < resolutionThreshold){
				electing = false;
				//count votes & change status to LEADING or FOLLWING
				counting = true;
			}
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

	private void voteNextMaster() {
		nextMaster = getNextMaster();
		if(nextMaster != null){
	        PutCommand voteNextMaster = new PutCommand("put");
	        voteNextMaster.setExecutor(YAConfig.exec);
	        voteNextMaster.execute(("com.yaconfig.node." + YAConfig.SERVER_ID + ".vote"),
	        		nextMaster.getServerId().getBytes());
		}
		counting = true;
	}

	private void countVotes() {
		
		HashMap<String,Integer> voteBox = new HashMap<String,Integer>();
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = (EndPoint)eps.get(key);
			if(ep.voteMaster != null){
				if(voteBox.get(ep.voteMaster) != null){
					Integer count = voteBox.get(ep.voteMaster);
					voteBox.put(ep.voteMaster, ++count);
				}else{
					voteBox.put(ep.voteMaster, 1);
				}
			}
		}		
		
		for(Iterator<String> it = voteBox.keySet().iterator();it.hasNext();){
			String key = it.next();
			Integer count = voteBox.get(key);
			
			if(count >= resolutionThreshold){
				//set master only if I am master,
				//other endpoint should be notified by "com.yaconfig.master" value
				setMaster(key);
				counting = false;
			}
		}
		
	}

	private void setMaster(String serverId) {
		if(serverId.equals(YAConfig.SERVER_ID)){
	        PutCommand setMaster = new PutCommand("put");
	        setMaster.setExecutor(YAConfig.exec);
	        setMaster.execute("com.yaconfig.master",YAConfig.SERVER_ID.getBytes());
			YAConfig.IS_MASTER = true;
			YAConfig.STATUS = EndPoint.EndPointStatus.LEADING;
			YAConfig.reportStatus();
		}
	}

	private EndPoint getNextMaster() {
		int minId = Integer.MAX_VALUE;
		int maxVID = Integer.MIN_VALUE;
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = (EndPoint)eps.get(key);
			int sid = Integer.parseInt(ep.getServerId());
			if(sid < minId && ep.VID >= maxVID
					&& ep.status == EndPoint.EndPointStatus.ELECTING 
					 	|| ep.status == EndPoint.EndPointStatus.LEADING
					 	|| ep.status == EndPoint.EndPointStatus.FOLLOWING){
				minId = sid;
			}
		}
		
		return eps.get(String.valueOf(minId));
	}

	public void setVotes(String key, String string) {
		eps.get(getServerIdFromKey(key)).voteMaster = string;
	}

	private class WatingMasterLeading implements Callable<Boolean>{

		private String masterId;
		
		public WatingMasterLeading(String masterId){
			this.masterId = masterId;
		}
		
		@Override
		public Boolean call() {
			for(;;){
				if(YAConfig.getEps().isLeading(masterId)){
					YAConfig.getEps().currentMaster = eps.get(masterId);
					System.out.println("the king is!!!!:" + masterId);
					if(!masterId.equals(YAConfig.SERVER_ID)){
						YAConfig.STATUS = EndPoint.EndPointStatus.FOLLOWING;
						YAConfig.reportStatus();
					}

					electing = false;
					counting = false;
					
					return true;
				}
			}			
		}
		
	}
	
	public void setCurrentMaster(String masterId) {
		//waiting for current master begin to lead.
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Boolean> future = executor.submit(new WatingMasterLeading(masterId));

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
        	electing = true;
            System.out.println("waiting master leading timeout");     
            future.cancel(true);  
        }finally{
			executor.shutdown();
		}
	
	}

	private boolean isLeading(String masterId) {
		if(masterId != null && eps.get(masterId) != null){
			return eps.get(masterId).status == EndPoint.EndPointStatus.LEADING;
		}
		return false;
	}
}
