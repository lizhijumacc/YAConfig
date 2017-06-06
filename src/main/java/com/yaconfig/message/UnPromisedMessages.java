package com.yaconfig.message;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.yaconfig.server.YAConfig;
import com.yaconfig.server.YAConfigProposer;

public class UnPromisedMessages{
	
	ConcurrentHashMap<Long,ArrayList<String>> promise;
	ConcurrentHashMap<Long,YAServerMessage> msgs; 
	YAConfigProposer server;
	
	public UnPromisedMessages(YAConfigProposer server){
		promise = new ConcurrentHashMap<Long,ArrayList<String>>();
		msgs = new ConcurrentHashMap<Long,YAServerMessage>();
		this.server = server;
	}
	
	public void countPromise(YAServerMessage msg){
		Long sequenceNum = msg.getSequenceNum();
		ArrayList<String> promisedServerIDs;

		synchronized(this){
			if(null != promise.get(sequenceNum)){
				promisedServerIDs = promise.get(sequenceNum);
				//TCP may re-send messages when timeout
				if(!alreadyPromised(promisedServerIDs,msg.getFromServerID())){		
					promisedServerIDs.add(msg.getFromServerID());
				}
			}else{
				promisedServerIDs = new ArrayList<String>();
				promisedServerIDs.add(msg.getFromServerID());
				promise.putIfAbsent(sequenceNum, promisedServerIDs);
			}
			
			//collect enough promise
			if(promisedServerIDs.size() == YAConfig.quorums){
				YAServerMessage sendMsg = new YAServerMessage(msg.getKey(),msg.getValue(),
						YAServerMessage.Type.COMMIT,msg.getSequenceNum());
				this.server.broadcastToQuorums(sendMsg);
				//should setVID when committed
				//this.server.setVID(yamsg.serverID,sequenceNum);
				promise.remove(sequenceNum);
				msgs.remove(sequenceNum);
			}
		}
	}
	
	private boolean alreadyPromised(ArrayList<String> promisedServerIDs, String serverID) {
		for(String s : promisedServerIDs){
			if(s.equals(serverID)){
				return true;
			}
		}
		return false;
	}

	public void push(YAServerMessage yamsg){
		msgs.putIfAbsent(yamsg.getSequenceNum(), yamsg);
	}
	
}
