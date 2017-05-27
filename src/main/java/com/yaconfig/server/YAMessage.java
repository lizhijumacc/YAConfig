package com.yaconfig.server;

import java.io.Serializable;

public class YAMessage implements Serializable {
	private static final long serialVersionUID = -2815202833060603706L;
	public String key;
	public byte[] value;
	public Type type;
	
	public enum Type{
		PUT,
		GET,
		GET_LOCAL,
	}
	
	public YAMessage(String key,byte[] value,Type type){
		this.key = key;
		this.value = value;
		this.type = type;
	}
}
