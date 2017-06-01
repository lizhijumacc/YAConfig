package com.yaconfig.server;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class UnPromisedMessages{
	
	ConcurrentHashMap<Long,ArrayList<String>> promise;
	ConcurrentHashMap<Long,YAMessage> msgs; 
	YAConfigServer server;
	
	public UnPromisedMessages(YAConfigServer server){
		promise = new ConcurrentHashMap<Long,ArrayList<String>>();
		msgs = new ConcurrentHashMap<Long,YAMessage>();
		this.server = server;
	}
	
	public void countPromise(YAMessage msg){
		Long sequenceNum = msg.sequenceNum;
		ArrayList<String> promisedServerIDs;
		
		synchronized(this){
			if(null != promise.get(sequenceNum)){
				promisedServerIDs = promise.get(sequenceNum);
				//TCP may re-send messages when timeout
				if(!alreadyPormised(promisedServerIDs,msg.serverID)){		
					promisedServerIDs.add(msg.serverID);
				}
			}else{
				promisedServerIDs = new ArrayList<String>();
				promisedServerIDs.add(msg.serverID);
				promise.putIfAbsent(sequenceNum, promisedServerIDs);
			}
			
			//collect enough promise
			if(promisedServerIDs.size() >= (YAConfig.quorums / 2) + 1){
				YAMessage yamsg = msgs.get(sequenceNum);
				yamsg.type = YAMessage.Type.COMMIT;
				this.server.broadcastToQuorums(yamsg);
				this.server.setVID(yamsg.serverID,sequenceNum);

				promise.remove(sequenceNum);
				msgs.remove(sequenceNum);
			}
		}
	}
	
	private boolean alreadyPormised(ArrayList<String> promisedServerIDs, String serverID) {
		for(String s : promisedServerIDs){
			if(s.equals(serverID)){
				return true;
			}
		}
		return false;
	}

	public void push(YAMessage yamsg){
		msgs.putIfAbsent(yamsg.sequenceNum, yamsg);
	}
	
}
