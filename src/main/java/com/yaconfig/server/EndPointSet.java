package com.yaconfig.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yaconfig.commands.PutCommand;

public class EndPointSet {
	public ConcurrentHashMap<String, EndPoint> eps;
	
	//elected master by quorums, if YAConfig.status is ELECTING,
	//the currentMaster is the last elected master
	public volatile EndPoint currentMaster;
	
	//master which I voted to
	public volatile EndPoint nextMaster;
	
	private Integer resolutionThreshold;
	
	private final Runnable masterListeningTask = new Runnable(){
		@Override
		public void run(){
			synchronized(eps){
				if(hasEnoughAliveEP()){
					//if there is no master in system or master conflict
					if(needElectMaster()){
						YAConfig.changeStatus(EndPoint.EndPointStatus.ELECTING);
						voteNextMaster();
					}else if(YAConfig.STATUS != EndPoint.EndPointStatus.LEADING){
						YAConfig.changeStatus(EndPoint.EndPointStatus.FOLLOWING);
						//init set master.
						if(currentMaster == null){
							setMasterLocal();
						}
					}
					
					
					//if master's heartbeat is timeout
					for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
						String key = it.next();
						EndPoint ep = (EndPoint)eps.get(key);
						
						if(System.currentTimeMillis() - ep.heartbeatTimestamp 
								> 2 * YAConfig.HEARTBEAT_INTVAL
								&& ep.status != EndPoint.EndPointStatus.DEAD){
							ep.status = EndPoint.EndPointStatus.DEAD;
							YAConfig.printImportant("CHECK EP DEAD", ep.getServerId() + " dead!");
							if(ep.equals(currentMaster)){
								electingService.execute(masterElectingTask);
							}
							//if next master which I am already voted is dead during election period
							//get new next master and vote again
							//if lack this re-vote process:
							//1. the master election resolution may never reached when the master
							//   which I voted is dead
							//2. if the master candidate which is elected by quorums is dead during 
							//   notify period, the currentMaster may never be set
							else if(ep.equals(nextMaster) 
									&& YAConfig.STATUS == EndPoint.EndPointStatus.ELECTING){
								voteNextMaster();
							}
						}
					}
				}
			}			
		}
	};
	
	private final Runnable masterElectingTask = new Runnable(){
		@Override
		public void run(){
			reElection();
		}
	};
	
	private final Runnable countVotesTask = new Runnable(){
		@Override
		public void run(){
			countVotes();
		}
	};
	
	private ScheduledExecutorService masterListeningService = Executors.newScheduledThreadPool(1);
	
	private ExecutorService electingService = Executors.newCachedThreadPool();
	
	public EndPointSet(){
		eps = new ConcurrentHashMap<String,EndPoint>();
		resolutionThreshold = (YAConfig.quorums / 2) + 1;
	}
	
	public void run() {
		masterListeningService.scheduleAtFixedRate(masterListeningTask, 1000, 1000, TimeUnit.MILLISECONDS);
	}
	
	//only add from config file.
	public void add(EndPoint e){
		if(!eps.contains(e)){
			e.heartbeatTimestamp = System.currentTimeMillis();
			eps.put(e.getServerId(), e);
		}
	}

	public void setEPStatus(String key, String status) {
		EndPoint ep = eps.get(getServerIdFromKey(key));
		
		if(null != ep){
			EndPoint.EndPointStatus preStatus = ep.status;
			ep.status = EndPoint.EndPointStatus.valueOf(status);
			if(preStatus != ep.status){
				printAllStatus();
			}
			ep.heartbeatTimestamp = System.currentTimeMillis();
		}
	}

	private String getServerIdFromKey(String key) {
		String epkey = key.substring(0, key.lastIndexOf("."));
		return epkey.substring(epkey.lastIndexOf(".") + 1);
	}

	public void setMasterLocal() {
		for(Entry<String,EndPoint> e : eps.entrySet()){
			EndPoint ep = e.getValue();
			if(ep.status == EndPoint.EndPointStatus.LEADING){
				currentMaster = ep;
			}
		}
	}

	protected boolean needElectMaster() {
		int leaderCount = 0;
		synchronized(eps){
			for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
				String key = it.next();
				EndPoint ep = (EndPoint)eps.get(key);
				if(ep.status == EndPoint.EndPointStatus.LEADING){
					leaderCount ++;
				}
			}
		}
		
		//if I am leading, but not get status report in this moments
		if(leaderCount == 0 && YAConfig.IS_MASTER){
			leaderCount++;
		}
		
		return leaderCount != 1;
	}
	
	public void printAllStatus(){
		System.out.println();
		System.out.println("-------------STATUS REPORT-------------------");
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = (EndPoint)eps.get(key);
			System.out.println(ep.getServerId() + " STATUS:" + String.valueOf(ep.status));
		}
		System.out.println("---------------------------------------------");
		System.out.println();
	}

	protected void reElection() {
		
			YAConfig.changeStatus(EndPoint.EndPointStatus.ELECTING);
        	//start status barrier to wait all the other endpoint status change to ELECTING
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
				YAConfig.changeStatus(EndPoint.EndPointStatus.FOLLOWING);
			}
			
			//if all the endpoint is waiting for new leader
			if(notwatingeps.size() == 0 && deadCount < resolutionThreshold){
				//count votes & change status to LEADING or FOLLWING
				electingService.execute(countVotesTask);
			}
		
	}

	private void voteNextMaster() {
		nextMaster = getNextMaster();
		if(nextMaster != null){
	        PutCommand voteNextMaster = new PutCommand("put");
	        voteNextMaster.setExecutor(YAConfig.exec);
	        voteNextMaster.execute(("com.yaconfig.node." + YAConfig.SERVER_ID + ".vote"),
	        		nextMaster.getServerId().getBytes());
	        electingService.execute(countVotesTask);
		}
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
			String serverId = it.next();
			Integer count = voteBox.get(serverId);
			if(count >= resolutionThreshold){
				//set master only if I am master,
				//other endpoint should be notified by "com.yaconfig.master" value
				setMaster(serverId);
			}
		}
		
	}

	private void setMaster(String serverId) {
		//if I am master, notify all the others & change myself status
		if(serverId.equals(YAConfig.SERVER_ID) && !YAConfig.IS_MASTER){
	        PutCommand setMaster = new PutCommand("put");
	        setMaster.setExecutor(YAConfig.exec);
	        setMaster.execute("com.yaconfig.master",YAConfig.SERVER_ID.getBytes());
			YAConfig.IS_MASTER = true;
			YAConfig.changeStatus(EndPoint.EndPointStatus.LEADING);
		}
		//if I am not master, set nextMaster to elected master
		//note that: here nextMaster is not pre-nexeMaster which I voted to
		//in case of the elected master dead during the notify master period.
		else if(!serverId.equals(YAConfig.SERVER_ID) && eps.get(serverId) != null){
			nextMaster = eps.get(serverId);
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
	
	public void setCurrentMaster(String masterId) {
		currentMaster = eps.get(masterId);
		YAConfig.printImportant("MASTER CHANGE", "curren master is:" + masterId);
		if(!masterId.equals(YAConfig.SERVER_ID)){
			YAConfig.changeStatus(EndPoint.EndPointStatus.FOLLOWING);
		}
	}

	private boolean isLeading(String masterId) {
		if(masterId != null && eps.get(masterId) != null){
			return eps.get(masterId).status == EndPoint.EndPointStatus.LEADING;
		}
		return false;
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
	
}
