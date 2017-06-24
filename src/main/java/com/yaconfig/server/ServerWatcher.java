package com.yaconfig.server;

import io.netty.channel.ChannelId;

public class ServerWatcher {
	private String rexStr;
	
	private ChangeListener changeListener;
	
	public ChannelId channelId;

	public ServerWatcher(String rex){
		this.rexStr = rex;
	}
	
	public ServerWatcher(String rex,ChannelId id){
		this.rexStr = rex;
		this.channelId = id;
	}
	
	public String getRexStr() {
		return rexStr;
	}

	public void setRexStr(String rexStr) {
		this.rexStr = rexStr;
	}

	public void setChangeListener(ChangeListener changeListener) {
		this.changeListener = changeListener;
	}
	
	public ChangeListener getChangeListener(){
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
		if(!(other instanceof ServerWatcher)){
			return false;
		}
		if(other == this){
			return true;
		}
		
		return ((ServerWatcher)other).channelId == this.channelId && rexStr.equals(((ServerWatcher)other).rexStr);
	}
}
