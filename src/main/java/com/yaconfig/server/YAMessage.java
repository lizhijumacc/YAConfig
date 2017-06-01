package com.yaconfig.server;

import java.io.Serializable;

public class YAMessage implements Serializable {
	private static final long serialVersionUID = -2815202833060603706L;
	public String key;
	public byte[] value;
	public Type type;
	public long timestamp;
	public long sequenceNum;
	public String serverID;
	
	public enum Type{
		PUT,
		PUT_NOPROMISE,
		GET,
		GET_LOCAL,
		PROMISE,
		COMMIT,
		NACK
	}
	
	public YAMessage(String key,byte[] value,Type type,long sequenceNum){
		this.key = key;
		this.value = value;
		this.type = type;
		this.sequenceNum = sequenceNum;
		this.serverID = YAConfig.SERVER_ID;
		timestamp = System.currentTimeMillis();
	}
	
	public String toString(){
		return  " from:" + this.serverID +
				" type:"+ String.valueOf(this.type) +
				" key:"+ this.key +
				" value:" + new String(this.value) + 
				//+ (this.timestamp/1000) % 100 + ":"
				" sequence number:" + this.sequenceNum;
	}
}
