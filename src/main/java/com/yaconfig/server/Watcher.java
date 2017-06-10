package com.yaconfig.server;

import io.netty.channel.ChannelId;

public class Watcher {
	private String rexStr;
	
	private IChangeListener changeListener;
	
	public ChannelId channelId;

	public Watcher(String rex){
		this.rexStr = rex;
	}
	
	public Watcher(String rex,ChannelId id){
		this.rexStr = rex;
		this.channelId = id;
	}
	
	public String getRexStr() {
		return rexStr;
	}

	public void setRexStr(String rexStr) {
		this.rexStr = rexStr;
	}

	public void setChangeListener(IChangeListener changeListener) {
		this.changeListener = changeListener;
	}
	
	public IChangeListener getChangeListener(){
		return changeListener;
	}
	
	public ChannelId getChannelId(){
		return channelId;
	}
	
	public void setChannelId(ChannelId id){
		this.channelId = id;
	}
	
	@Override
	public int hashCode(){
		if(channelId != null){
			return channelId.hashCode() + rexStr.hashCode();
		}
		return rexStr.hashCode();
	}
	
	@Override
	public boolean equals(Object other){
		if(!(other instanceof EndPoint)){
			return false;
		}
		if(other == this){
			return true;
		}
		
		return ((Watcher)other).channelId == this.channelId && rexStr.equals(((Watcher)other).rexStr);
	}
}
