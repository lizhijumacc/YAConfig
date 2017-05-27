package com.yaconfig.commands;

public class GetCommand extends Command{

	public GetCommand(String name) {
		super(name);
	}
	
	public byte[] executeQuery(String name){
		return exec.get(name);
	}
}
