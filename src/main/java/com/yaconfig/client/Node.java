package com.yaconfig.client;

public class Node {
	private String ip;
	private int port;
	
	public Node(String host){
		String ip = host.substring(0,host.indexOf(":"));
		int port = Integer.parseInt(host.substring(host.indexOf(":") + 1));
		
		this.ip = ip;
		this.port = port;
	}
	
	public String getIp(){
		return ip;
	}
	
	public int getPort(){
		return port;
	}
	
	public void setIp(String ip){
		this.ip = ip;
	}
	
	public void setPort(int port){
		this.port = port;
	}
}
