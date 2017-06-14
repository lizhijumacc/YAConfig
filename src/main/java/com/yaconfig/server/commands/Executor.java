package com.yaconfig.server.commands;

import java.io.Serializable;

import com.yaconfig.server.EndPoint;
import com.yaconfig.server.YAConfig;
import com.yaconfig.server.message.YAServerMessage;
import com.yaconfig.server.storage.YAHashMap;

public class Executor implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7575979837994253259L;

	private YAConfig yaconfig;
	
	public Executor(YAConfig yaconfig){
		this.yaconfig = yaconfig;
	}
	
	public void put(String key,byte[] value,boolean withoutPromise) {
		YAServerMessage yaMessage;
		
		if(key == null){
			return;
		}
		
		if(yaconfig.isSystemMessage(key)){
			yaMessage = new YAServerMessage(key,value,YAServerMessage.Type.PUT_NOPROMISE,-1);
			yaconfig.broadcastToQuorums(yaMessage);
			return;
		}
		
		
		if(withoutPromise){
			yaMessage = new YAServerMessage(key,value,YAServerMessage.Type.PUT_NOPROMISE,0);
		}else{
			yaMessage = new YAServerMessage(key,value,YAServerMessage.Type.PUT,yaconfig.getUnpromisedNum());
		}
		if(YAConfig.STATUS == EndPoint.Status.FOLLOWING){
			yaconfig.redirectToMaster(yaMessage);
		}else if(YAConfig.STATUS == EndPoint.Status.LEADING){
			yaconfig.broadcastToQuorums(yaMessage);
		}
	
	}

	public byte[] get(String key){
		return YAHashMap.getInstance().get(key);
	}
}
