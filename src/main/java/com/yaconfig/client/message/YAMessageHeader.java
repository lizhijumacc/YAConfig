package com.yaconfig.client.message;

public class YAMessageHeader {
	public int type;
	public int version = 1;
	
	public int length(){
		return 4 + 4;
	}
}
