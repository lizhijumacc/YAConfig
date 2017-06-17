package com.yaconfig.client.message;

public class YAMessageHeader {
	public int type;
	public int version = 1;
	public long id;
	
	public int length(){
		return 4 + 4 + 8;
	}
}
