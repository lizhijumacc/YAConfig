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
	
	public void put(String key,byte[] value,boolean withoutPromise) {
		YAMessage yaMessage;
		
		if(key == null){
			return;
		}
		
		if(yaconfig.isSystemMessage(key)){
			yaMessage = new YAMessage(key,value,YAMessage.Type.PUT_NOPROMISE,-1);
			yaconfig.broadcastToQuorums(yaMessage);
			return;
		}
		
		if(yaconfig.statusEquals(EndPoint.Status.FOLLOWING)){
			if(withoutPromise){
				yaMessage = new YAMessage(key,value,YAMessage.Type.PUT_NOPROMISE,0);
			}else{
				yaMessage = new YAMessage(key,value,YAMessage.Type.PUT,0);
			}
			yaconfig.redirectToMaster(yaMessage);
		}else{
			return;
		}			
		
		
	}

	public byte[] get(String key){
		return YAHashMap.getInstance().get(key);
	}
}
