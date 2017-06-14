package com.yaconfig.server.message;

import com.yaconfig.client.message.YAMessage;
import com.yaconfig.server.YAConfig;

public class YAServerMessage {
	
	YAServerMessageHeader header;
	
	String key;
	byte[] value;

	public class Type{
		public static final int PUT = 0;
		public static final int PUT_NOPROMISE = 1;
		public static final int GET = 2;
		public static final int GET_LOCAL = 3;
		public static final int PROMISE = 4;
		public static final int COMMIT = 5;
		public static final int NACK = 6;
	}
	
	public YAServerMessage(String key,byte[] value,int type,long sequenceNum){
		this.key = key;
		this.value = value;
		this.header = new YAServerMessageHeader();
		this.header.type = type;
		this.header.sequenceNum = sequenceNum;
		this.header.serverID = YAConfig.SERVER_ID;
		this.header.serverStatus = YAConfig.STATUS;
	}

	public YAServerMessage(YAServerMessageHeader header,String key,byte[] value){
		this.header = header;
		this.key = key;
		this.value = value;
	}
	
	public YAServerMessage(YAMessage yamsg,long sequenceNum) throws ClassCastException{
		this.key = yamsg.key;
		this.value = yamsg.value;
		this.header = new YAServerMessageHeader();
		if(yamsg.getType() < 4){
			this.header.type = yamsg.getType();
		}else{
			throw new ClassCastException("type is not compatable.");
		}
		
		this.header.sequenceNum = sequenceNum;
		this.header.serverID = YAConfig.SERVER_ID;
		this.header.serverStatus = YAConfig.STATUS;
	}
	
	public int getType(){
		return header.type;
	}
	
	public long getSequenceNum(){
		return header.sequenceNum;
	}
	
	public String getFromServerID(){
		return header.serverID;
	}
	
	public String getKey(){
		return key;
	}
	
	public byte[] getValue(){
		return value;
	}
	
	public String toString(){
		return  " from:" + this.header.serverID +
				" type:"+ String.valueOf(this.header.type) +
				" key:"+ this.key +
				" value:" + new String(this.value) + 
				" sequence number:" + this.header.sequenceNum;
	}

	public int length() {
		return header.length() + key.getBytes().length + value.length;
	}
}
