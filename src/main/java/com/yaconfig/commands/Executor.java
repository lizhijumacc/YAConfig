package com.yaconfig.commands;

import java.io.Serializable;

import com.yaconfig.server.EndPoint;
import com.yaconfig.server.YAConfig;
import com.yaconfig.server.YAMessage;
import com.yaconfig.storage.YAHashMap;

public class Executor implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7575979837994253259L;

	public void put(YAMessage yaMessage, PutCallback callback) {
		if(yaMessage.type == YAMessage.Type.PUT){
			if(YAConfig.STATUS == EndPoint.EndPointStatus.ELECTING
					||YAConfig.STATUS == EndPoint.EndPointStatus.INIT
					||YAConfig.STATUS == EndPoint.EndPointStatus.LEADING){
				YAConfig.broadcastToQuorums(yaMessage);
			}else if(YAConfig.STATUS == EndPoint.EndPointStatus.FOLLOWING){
				YAConfig.redirectToMaster(yaMessage);
			}else{
				return;
			}			
		}
		
		if(callback != null){
			callback.callback();
		}
	}
	
	public byte[] get(String key){
		return YAHashMap.getInstance().get(key);
	}
}
