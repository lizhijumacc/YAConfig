package com.yaconfig.server;

public class Watcher {
	private String rexStr;
	
	private IChangeListener changeListener;

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
}
