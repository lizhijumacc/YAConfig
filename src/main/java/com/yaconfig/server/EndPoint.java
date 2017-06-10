package com.yaconfig.server;

import io.netty.channel.ChannelId;

public class EndPoint {
	
	//the current replica version ID in this endpoint
	public long VID = 0;

	private String ip;
	
	private String port;
	
	private String serverId;
	
	public String voteMaster;
	
	public ChannelId channelId;
	
	public class Status{
		public static final int ALIVE = 0;
		public static final int DEAD = 1;
		public static final int ELECTING = 2;
		public static final int LEADING = 3;
		public static final int FOLLOWING = 4;
		public static final int INIT = 5;
		public static final int UNKOWN = 6;
	}
	
	public volatile int status = Status.UNKOWN;
	
	public EndPoint(String serverId,String host){
		String[] sp = host.split(":");
		this.setIp(sp[0]);
		this.setPort(sp[1]);
		this.setServerId(serverId);
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}
	
	public ChannelId getChannelId(){
		return this.channelId;
	}
	
	public void setChannelId(ChannelId id){
		this.channelId = id;
	}
	
	@Override
	public int hashCode(){
		return (host() + this.serverId).hashCode();
	}
	
	@Override
	public boolean equals(Object other){
		if(!(other instanceof EndPoint)){
			return false;
		}
		if(other == this){
			return true;
		}
		return this.ip.equals(((EndPoint)other).getIp()) 
				&& this.port.equals(((EndPoint)other).getPort())
				&& this.serverId.equals(((EndPoint)other).getServerId()); 
	}

	public String host() {
		return ip + ":" + port;
	}

	public String getServerId() {
		return serverId;
	}

	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	
}
