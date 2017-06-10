package com.yaconfig.server;

import io.netty.channel.ChannelId;

public class Watcher {
	private String rexStr;
	
	private IChangeListener changeListener;
	
	public ChannelId channelId;

	public Watcher(String rex){
		this.rexStr = rex;
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
}
