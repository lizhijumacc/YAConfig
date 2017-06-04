package com.yaconfig.message;

public class YAMessageHeader {
	public int type;
	public long sequenceNum;
	public String serverID;
	public int serverStatus;
	public int version = 1;
	
	public int length(){
		return 4 + 8 + serverID.getBytes().length + 4 + 4;
	}
}
