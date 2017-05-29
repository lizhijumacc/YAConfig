package com.yaconfig.server;

public class EndPoint {
	
	//the current replica version ID in this endpoint
	public  int VID = Integer.MIN_VALUE;

	private String ip;
	
	private String port;
	
	private String serverId;
	
	public long heartbeatTimestamp;
	
	public String voteMaster;
	
	public enum EndPointStatus{
		ALIVE,
		DEAD,
		ELECTING,
		LEADING,
		FOLLOWING,
		INIT,
		UNKOWN;
	}
	
	public volatile EndPointStatus status = EndPointStatus.UNKOWN;
	
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
