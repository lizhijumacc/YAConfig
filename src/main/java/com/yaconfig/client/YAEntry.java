package com.yaconfig.client;

public class YAEntry implements java.util.Map.Entry<String, byte[]>{

	public String key;
	
	public byte[] value;

	public YAEntry(String key,byte[] value){
		this.key = key;
		this.value = value;
	}
	
	@Override
	public String getKey() {
		return key;
	}

	@Override
	public byte[] getValue() {
		return value;
	}

	@Override
	public byte[] setValue(byte[] value) {
		this.value = value;
		return this.value;
	}

}
