package com.yaconfig.client.message;

public class YAMessage {
	public class Type{
		//commmand
		public static final int PUT = 0;
		public static final int PUT_NOPROMISE = 1;
		public static final int GET = 2;
		public static final int GET_LOCAL = 3;
		public static final int WATCH = 4;
		public static final int UNWATCH = 5;
		
		//events
		public static final int ADD = 6;
		public static final int DELETE = 7;
		public static final int UPDATE = 8;
		
		//response
		public static final int ACK = 9;
		public static final int NACK = 10;
	}
	
	public YAMessageHeader header;
	
	public String key;
	
	public byte[] value;
	
	public YAMessage(int type,String key,byte[] value){
		this.key = key;
		this.value = value;
		this.header = new YAMessageHeader();
		this.header.type = type;
		this.header.id = System.nanoTime() + this.hashCode();
	}
	
	public String getKey(){
		return key;
	}
	
	public byte[] getValue(){
		return value;
	}
	
	public int getType(){
		return header.type;
	}
	
	public void setKey(String key){
		this.key = key;
	}
	
	public void setValue(byte[] value){
		this.value = value;
	}
	
	public long getId(){
		return this.header.id;
	}
	
	public void setId(long id){
		this.header.id = id;
	}
	
	public int length(){
		return key.getBytes().length + value.length + header.length();
	}
	
	@Override
	public String toString(){
		return "Type:" + this.header.type + "KEY:" + new String(this.key) + " VALUE:" + new String(this.value);
	}
}
