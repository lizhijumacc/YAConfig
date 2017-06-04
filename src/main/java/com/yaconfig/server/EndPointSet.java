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
	
	//elected master by quorums, if yaconfig.status is ELECTING,
	//the currentMaster is the last elected master
	public volatile EndPoint currentMaster;
	
	//master which I voted to
	public volatile EndPoint nextMaster;
	
	private Integer resolutionThreshold;
	
	private YAConfig yaconfig;
	
	private final Runnable masterListeningTask = new Runnable(){
		@Override
		public void run(){
			synchronized(eps){
				if(hasEnoughAliveEP()){
					//if there is no master in system or master conflict
					if(needElectMaster()){
						yaconfig.changeStatus(EndPoint.Status.ELECTING);
						voteNextMaster();
					}else if(!yaconfig.statusEquals(EndPoint.Status.LEADING)){
						yaconfig.changeStatus(EndPoint.Status.FOLLOWING);
						//init set master.
						if(currentMaster == null){
							setMasterLocal();
						}
					}
					
					//if master's heartbeat is timeout
					for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
						String key = it.next();
						EndPoint ep = (EndPoint)eps.get(key);
						
						if(ep.status == EndPoint.Status.DEAD){
							//during currentMaster is dead & I am master is not
							//notify to others,the new master's status may change
							//LEADING to ELECTING again.so must add needElectMaster()
							if(ep.equals(currentMaster) && needElectMaster()){
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
									&& yaconfig.statusEquals(EndPoint.Status.ELECTING)){
								voteNextMaster();
							}
						}
					}
				}else{
					yaconfig.changeStatus(EndPoint.Status.ELECTING);
					voteNextMaster();
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
	
	public EndPointSet(YAConfig yaconfig){
		eps = new ConcurrentHashMap<String,EndPoint>();
		resolutionThreshold = (YAConfig.quorums / 2) + 1;
		this.yaconfig = yaconfig;
	}
	
	public void run() {
		masterListeningService.scheduleAtFixedRate(masterListeningTask, 1000, 1000, TimeUnit.MILLISECONDS);
	}
	
	//only add from config file.
	public void add(EndPoint e){
		if(!eps.contains(e)){
			eps.put(e.getServerId(), e);
		}
	}

	public void setEPStatus(String key, int status) {
		EndPoint ep = eps.get(getServerIdFromKey(key));
		
		if(null != ep && !ep.getServerId().equals(YAConfig.SERVER_ID)){
			synchronized(eps){
				int preStatus = ep.status;
				ep.status = status;
				if(preStatus != ep.status){
					printAllStatus();
				}
				
			}
		}
	}

	private String getServerIdFromKey(String key) {
		String epkey = key.substring(0, key.lastIndexOf("."));
		return epkey.substring(epkey.lastIndexOf(".") + 1);
	}

	public void setMasterLocal() {
		for(Entry<String,EndPoint> e : eps.entrySet()){
			EndPoint ep = e.getValue();
			if(ep.status == EndPoint.Status.LEADING){
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
				if(ep.status == EndPoint.Status.LEADING){
					leaderCount ++;
				}
			}
		}
		
		
		//if I am leading, but not get status report in this moments
		if(leaderCount == 0 && yaconfig.IS_MASTER){
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
			yaconfig.changeStatus(EndPoint.Status.ELECTING);
        	//start status barrier to wait all the other endpoint status change to ELECTING
			//proposal the node as master which has minimum SERVER_ID
			voteNextMaster();
			HashSet<EndPoint> notwatingeps = new HashSet<EndPoint>();
			int deadCount = 0;
			for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
				String key = it.next();
				EndPoint ep = (EndPoint)eps.get(key);
				if(ep.status != EndPoint.Status.DEAD || 
						ep.status != EndPoint.Status.ELECTING){
					notwatingeps.add(ep);
				}
				
				if(ep.status == EndPoint.Status.DEAD || 
						ep.status == EndPoint.Status.UNKOWN){
					deadCount++;
				}
			}
			
			
	        //if the old leader is fake dead(because of networking jitter) in this waiting period,
	        //the condition may never be achieved,so break the forever loop when old leader online again.
			if(notwatingeps.contains(currentMaster)
					&& currentMaster.status == EndPoint.Status.LEADING
					&& !yaconfig.IS_MASTER){
				yaconfig.changeStatus(EndPoint.Status.FOLLOWING);
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
	        voteNextMaster.setExecutor(yaconfig.exec);
	        voteNextMaster.execute((YAConfig.SYSTEM_PERFIX + ".node." + YAConfig.SERVER_ID + ".vote"),
	        		(nextMaster.getServerId() + "///" + nextMaster.VID).getBytes(),true);
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
				//other endpoint should be notified by "SYSTEM_PERFIX.master" value
				setMaster(serverId);
			}
		}
		
	}

	private void setMaster(String serverId) {
		//if I am master, notify all the others & change myself status
		if(serverId.equals(YAConfig.SERVER_ID) && !yaconfig.IS_MASTER){
	        PutCommand setMaster = new PutCommand("put");
	        setMaster.setExecutor(yaconfig.exec);
	        setMaster.execute(YAConfig.SYSTEM_PERFIX + ".master",YAConfig.SERVER_ID.getBytes(),true);
			yaconfig.IS_MASTER = true;
			YAConfig.unpromisedNum = eps.get(YAConfig.SERVER_ID).VID;
			yaconfig.changeStatus(EndPoint.Status.LEADING);
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
					&& ep.status == EndPoint.Status.ELECTING 
					 	|| ep.status == EndPoint.Status.LEADING
					 	|| ep.status == EndPoint.Status.FOLLOWING){
				minId = sid;
			}
		}
		
		return eps.get(String.valueOf(minId));
	}

	public void setVotes(String key, String string) {
		EndPoint ep = eps.get(getServerIdFromKey(key));
		if(ep != null){
			ep.voteMaster = string.substring(0,string.lastIndexOf("///"));
			ep.VID = Long.parseLong(string.substring(string.lastIndexOf("///") + 3));
		}
	}
	
	public void setCurrentMaster(String masterId) {
		currentMaster = eps.get(masterId);
		YAConfig.printImportant("MASTER CHANGE", "curren master is:" + masterId);
		if(!masterId.equals(YAConfig.SERVER_ID)){
			yaconfig.changeStatus(EndPoint.Status.FOLLOWING);
		}
	}

	
	@SuppressWarnings("unused")
	private boolean isLeading(String masterId) {
		if(masterId != null && eps.get(masterId) != null){
			return eps.get(masterId).status == EndPoint.Status.LEADING;
		}
		return false;
	}
	
	private boolean hasEnoughAliveEP() {
		Integer countDead = 0;
		for(Iterator<String> it = eps.keySet().iterator();it.hasNext();){
			String key = it.next();
			EndPoint ep = (EndPoint)eps.get(key);
			if(ep.status == EndPoint.Status.DEAD){
				countDead++;
			}
		}
		
		return countDead < resolutionThreshold;
	}

	public void setVID(String serverID, Long sequenceNum) {
		synchronized(eps){
			EndPoint ep = eps.get(serverID);
			ep.VID = sequenceNum;
		}
	}

	public void peerDead(String ip, String port) {
		if(ip != null && port != null){
			for(Entry<String,EndPoint> e : eps.entrySet()){
				EndPoint ep = e.getValue();
				if(ep.getIp().equals(ip) && ep.getPort().equals(port)){
					//no report to others
					if(ep.status != EndPoint.Status.DEAD){
						ep.status = EndPoint.Status.DEAD;
						YAConfig.printImportant("CHECK EP DEAD", ep.getServerId() + " dead!");
						System.out.println(YAConfig.VID);
					}
				}
			}
		}
	}

	public void changeMyselfStatus(int newStatus) {
		synchronized(eps){
			for(Entry<String,EndPoint> e : eps.entrySet()){
				EndPoint ep = e.getValue();
				if(ep.getServerId().equals(YAConfig.SERVER_ID)){
					ep.status = newStatus;
				}
			}
		}
	}
	
}
