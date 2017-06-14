package com.yaconfig.server.message;

public class YAServerMessageHeader {
	public int type;
	public long sequenceNum;
	public String serverID;
	public int serverStatus;
	public int version = 1;
	
	public int length(){
		return 4 + 8 + serverID.getBytes().length + 4 + 4;
	}
}
