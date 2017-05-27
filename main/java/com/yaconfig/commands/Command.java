package com.yaconfig.commands;

import java.io.Serializable;

public abstract class Command implements Serializable {
	
	private static final long serialVersionUID = 4221664106089664956L;

	protected String name;
	
	protected String args;
	
	
	protected Executor exec;
	
	public Command(String name){
		this.name = name;
	}
	
	public String getName(){
		return this.name;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public String getArgs(){
		return this.name;
	}
	
	public void setArgs(String args){
		this.args = args;
	}
	
	public Command setExecutor(Executor exec){
		this.exec = exec;
		return this;
	}
}
