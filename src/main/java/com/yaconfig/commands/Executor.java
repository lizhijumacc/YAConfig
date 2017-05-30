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

	private YAConfig yaconfig;
	
	public Executor(YAConfig yaconfig){
		this.yaconfig = yaconfig;
	}
	
	public void put(YAMessage yaMessage, PutCallback callback) {
		if(yaMessage.type == YAMessage.Type.PUT){
			if(yaconfig.STATUS == EndPoint.Status.ELECTING
					||yaconfig.STATUS == EndPoint.Status.INIT
					||yaconfig.STATUS == EndPoint.Status.LEADING){
				yaconfig.broadcastToQuorums(yaMessage);
			}else if(yaconfig.STATUS == EndPoint.Status.FOLLOWING){
				yaconfig.redirectToMaster(yaMessage);
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
