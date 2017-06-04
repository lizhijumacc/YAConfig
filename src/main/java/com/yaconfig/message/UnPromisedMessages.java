package com.yaconfig.message;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.yaconfig.server.YAConfig;
import com.yaconfig.server.YAConfigServer;

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
				YAMessage sendMsg = new YAMessage(msg.getKey(),msg.getValue(),
						YAMessage.Type.COMMIT,msg.getSequenceNum());
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

	public void push(YAMessage yamsg){
		msgs.putIfAbsent(yamsg.getSequenceNum(), yamsg);
	}
	
}
